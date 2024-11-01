package partition

type ConfigState struct {

	// Ignore if true, ignore comparing ConfigState
	Ignore bool

	// Core section of git config file
	Core *ConfigStateCore

	// Remotes of git config file
	Remotes []ConfigStateRemote
}

type ConfigStateCore struct {
	//repositoryformatversion = 0
	//filemode = true
	//bare = true
	//ignorecase = true
	//precomposeunicode = true
	Bare bool
}

type ConfigStateRemote struct {
	//[remote "offload"]
	//url = file:///Users/peijian/Gitlab/playground/promise-orgin/local-promisor/mb-file-gamer-promisor.git
	//promisor = true
	//fetch = +refs/heads/*:refs/remotes/gsremote/*
	Name     string
	URL      string
	Promisor bool
	Fetch    string
}

// ConfigState parse
