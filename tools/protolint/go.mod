module gitlab.com/gitlab-org/gitaly/tools/protolint

go 1.22

toolchain go1.22.0

require github.com/yoheimuta/protolint v0.50.5

require (
	github.com/BurntSushi/toml v1.4.0 // indirect
	github.com/chavacava/garif v0.1.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/gertd/go-pluralize v0.2.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-plugin v1.6.1 // indirect
	github.com/hashicorp/yamux v0.1.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mitchellh/go-testing-interface v1.14.1 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/yoheimuta/go-protoparser/v4 v4.11.0 // indirect
	golang.org/x/net v0.25.0 // indirect
	golang.org/x/sys v0.20.0 // indirect
	golang.org/x/text v0.15.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20240528184218-531527333157 // indirect
	google.golang.org/grpc v1.65.0 // indirect
	google.golang.org/protobuf v1.34.2 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

exclude (
	// GO-2022-1059, GO-2021-0113, GO-2020-0015
	golang.org/x/text v0.3.0
	golang.org/x/text v0.3.3
	golang.org/x/text v0.3.5
	golang.org/x/text v0.3.7
)
