package trace2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/internal/structerr"
	"gitlab.com/gitlab-org/gitaly/v16/internal/tracing"
)

// Parse parses the events generated by Git Trace2 API into a tree data structure.
//
// Git Trace2 produces a flat list of events. They are sorted in chronological order. Each event
// describes a certain sub operation, including some relative data and metadata. Some events, such
// as "region_enter" or "cmd_start", indicate a new subtree in which the consecutive events belong
// to. Correspondingly, closing events such as "region_leave" or "atexit", exits the current
// section. Trace2 also captures the events of children processes.
//
// By default, all events include "time", "file", and "line" fields. Those fields increase the size
// and processing overhead significantly. So, we set GIT_TRACE2_BRIEF environment variable to omit
// such information. Only the time from the initial events of main process or sub processes
// contains the absolute. The times of other events can be inferred from the time difference
// relative to the first event ("t_abs" field) or the current section ("t_rel" field).
//
// Apart from the processing events, Trace2 API also exposes useful statistical information. They
// are stored in "data" and "data_json" events under "key" and "value" fields. They are particularly
// useful to sample and expose internal Git metrics.
//
// The result of the parsing process is a root Trace node of the tree. The root node is a dummy node,
// not a part of the original events. So, it's recommended to skip this node when walking.
//
// For more information, please visit Trace2 API: https://git-scm.com/docs/api-trace2
func Parse(ctx context.Context, reader io.Reader) (*Trace, error) {
	span, _ := tracing.StartSpanIfHasParent(ctx, "trace2.parse", nil)
	defer span.Finish()

	decoder := json.NewDecoder(reader)
	p := &parser{decoder: decoder}
	return p.parse()
}

type parser struct {
	root        *Trace
	currentNode *Trace
	decoder     *json.Decoder
}

// timeLayout defines the absolute timestamp format of trace2 event
const timeLayout = "2006-01-02T15:04:05.000000Z"

// jsonEvent is a simplified version of Trace2 event format. Each event may have different fields
// according to event type. The full format can be found here:
// https://git-scm.com/docs/api-trace2#_event_format
type jsonEvent struct {
	Name      string           `json:"event"`
	Category  string           `json:"category"`
	Label     string           `json:"label"`
	Thread    string           `json:"thread"`
	DataKey   string           `json:"key"`
	DataValue *json.RawMessage `json:"value"`
	// Absolute time of the event
	Time string `json:"time"`
	// Time difference in second time since the program starts
	TimeAbsSeconds float64 `json:"t_abs"`
	// Time difference in second time in seconds relative to the start of the current region
	TimeRelSeconds float64  `json:"t_rel"`
	Argv           []string `json:"argv"`
	ChildID        int      `json:"child_id"`
	Msg            string   `json:"msg"`
	Code           int      `json:"code"`
	Exe            string   `json:"exe"`
	Evt            string   `json:"evt"`
	Worktree       string   `json:"worktree"`
}

var ignoredEvents = map[string]struct{}{
	"cmd_name": {},
	"exit":     {},
}

// parse receives a reader object and returns the root node of the tree. The parser reads line by
// line. Each line contains an event in JSON format. It doesn't rewind the reader.
func (p *parser) parse() (*Trace, error) {
	for {
		event, err := p.readEvent()
		if err != nil {
			return nil, fmt.Errorf("reading event: %w", err)
		}
		if event == nil {
			return p.root, nil
		}
		if err := p.parseEvent(event); err != nil {
			p.root = nil
			return nil, structerr.NewInternal("processing event: %w", err).WithMetadata("event", event)
		}
	}
}

func (p *parser) readEvent() (*jsonEvent, error) {
	var event jsonEvent
	if err := p.decoder.Decode(&event); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("decoding event: %w", err)
	}
	return &event, nil
}

func (p *parser) parseEvent(event *jsonEvent) error {
	if _, ok := ignoredEvents[event.Name]; ok {
		return nil
	}

	if p.root != nil && p.currentNode == nil {
		// The situation where there are more leaving events than starting ones. That makes
		// the currentNode exits the root node of the tree while moving upward. For example:
		// [start, region_start, region_start, region_leaving, region_leaving, region_leaving]
		return fmt.Errorf("unmatched leaving event")
	}

	var trace *Trace
	eventTime, err := p.parseEventTime(p.currentNode, event)
	if err != nil {
		return fmt.Errorf("parsing event time: %w", err)
	}

	if p.root == nil {
		trace = &Trace{Thread: event.Thread, StartTime: eventTime, Name: "root"}
		p.root = trace
		p.currentNode = p.root
	}

	// Leaving events, don't create trace
	switch event.Name {
	case "atexit":
		p.currentNode.FinishTime = eventTime
		p.currentNode.SetMetadata("code", fmt.Sprintf("%d", event.Code))
		return nil
	case "child_exit":
		p.currentNode.FinishTime = eventTime
		p.currentNode.SetMetadata("code", fmt.Sprintf("%d", event.Code))
		p.currentNode = p.currentNode.Parent
		return nil
	case "region_leave":
		p.currentNode.FinishTime = eventTime
		p.currentNode = p.currentNode.Parent
		return nil
	}

	trace = &Trace{
		ChildID:    p.currentNode.ChildID,
		Thread:     event.Thread,
		StartTime:  eventTime,
		FinishTime: eventTime,
		Parent:     p.currentNode,
		Depth:      p.currentNode.Depth + 1,
	}
	if event.Msg != "" {
		trace.SetMetadata("msg", event.Msg)
	}
	p.currentNode.Children = append(p.currentNode.Children, trace)

	switch event.Name {
	case "version":
		trace.setName([]string{event.Name, event.Category, event.Label})
		trace.SetMetadata("exe", event.Exe)
		trace.SetMetadata("evt", event.Evt)
	case "def_repo":
		trace.setName([]string{event.Name, event.Category, event.Label})
		trace.SetMetadata("worktree", event.Worktree)
	case "start":
		trace.setName([]string{event.Name, event.Category, event.Label})
		trace.SetMetadata("argv", strings.Join(event.Argv, " "))
	case "child_start":
		trace.setName([]string{event.Name, event.Category, event.Label})
		trace.SetMetadata("argv", strings.Join(event.Argv, " "))
		trace.ChildID = fmt.Sprintf("%d", event.ChildID)
		p.currentNode = trace
	case "region_enter":
		trace.setName([]string{event.Category, event.Label})
		p.currentNode = trace
	case "data":
		trace.setName([]string{event.Name, event.Category, event.Label, event.DataKey})
		if event.DataValue != nil {
			var data string
			// When the event name is "data", we can unmarshal the data. This allows
			// easy data access later
			err := json.Unmarshal(*event.DataValue, &data)
			if err != nil {
				return fmt.Errorf("mismatched data value: %w", err)
			}
			trace.SetMetadata("data", data)
		}
	case "data_json":
		trace.setName([]string{event.Name, event.Category, event.Label, event.DataKey})
		if event.DataValue != nil {
			trace.SetMetadata("data", string(*event.DataValue))
		}
	default:
		trace.setName([]string{event.Name, event.Category, event.Label})
	}
	return nil
}

func (p *parser) parseEventTime(parent *Trace, event *jsonEvent) (time.Time, error) {
	// Absolute time. If GIT_TRACE2_BRIEF env variable is set this field is attached to the
	// first event only. Other event's time must be inferred from time diff (TimeAbsSeconds and TimeRelSeconds)
	if event.Time != "" {
		return time.Parse(timeLayout, event.Time)
	}

	// Absolute time difference from the root
	if event.TimeAbsSeconds != 0 {
		if p.root == nil {
			return time.Time{}, fmt.Errorf("initial time is missing")
		}
		return p.addTime(p.root.StartTime, event.TimeAbsSeconds), nil
	}

	var parentTime time.Time
	if parent != nil {
		parentTime = parent.StartTime
	} else {
		if p.root == nil {
			return time.Time{}, fmt.Errorf("initial time is missing")
		}
		parentTime = p.root.StartTime
	}

	// Relative time difference from its parent
	if event.TimeRelSeconds != 0 {
		return p.addTime(parentTime, event.TimeRelSeconds), nil
	}

	// If an event doesn't have either TimeAbsSeconds and TimeRelSeconds, infer the time from its prior sibling
	if parent != nil && len(parent.Children) != 0 {
		return parent.Children[len(parent.Children)-1].FinishTime, nil
	}

	// If the event is the only child without any further information, use its parent time
	return parentTime, nil
}

func (p *parser) addTime(t time.Time, diffSeconds float64) time.Time {
	return t.Add(time.Duration(diffSeconds * float64(time.Second)))
}
