PREFIX=/usr/local
PKG=gitlab.com/gitlab-org/gitaly
BUILD_DIR=${CURDIR}
BIN_BUILD_DIR=${BUILD_DIR}/_build/bin
PKG_BUILD_DIR:=${BUILD_DIR}/_build/src/${PKG}
CMDS:=$(shell cd cmd && ls)
TEST_REPO=internal/testhelper/testdata/data/gitlab-test.git
VERSION=$(shell git describe)-$(shell date -u +%Y%m%d.%H%M%S)

export GOPATH=${BUILD_DIR}/_build
export GO15VENDOREXPERIMENT=1
export PATH:=${GOPATH}/bin:$(PATH)

.NOTPARALLEL:

.PHONY: all
all: build

.PHONY: ${BUILD_DIR}/_build
${BUILD_DIR}/_build:
	mkdir -p $@/src/${PKG}
	tar -cf - --exclude _build --exclude .git . | (cd $@/src/${PKG} && tar -xf -)
	touch $@

build:	clean-build ${BUILD_DIR}/_build $(shell find . -name '*.go' -not -path './vendor/*' -not -path './_build/*')
	rm -f -- "${BIN_BUILD_DIR}/*"
	go install -ldflags "-X main.version=${VERSION}" ${PKG}/cmd/...
	cp ${BIN_BUILD_DIR}/* ${BUILD_DIR}/

install: build
	mkdir -p $(DESTDIR)${PREFIX}/bin/
	cd ${BIN_BUILD_DIR} && install ${CMDS} ${DESTDIR}${PREFIX}/bin/

verify: lint check-formatting govendor-status notice-up-to-date

check-formatting: install-developer-tools
	go run _support/gofmt-all.go -n

govendor-status: ${BUILD_DIR}/_build install-developer-tools
	cd ${PKG_BUILD_DIR} && govendor status

${TEST_REPO}:
	git clone --bare https://gitlab.com/gitlab-org/gitlab-test.git $@

test: clean-build ${TEST_REPO} ${BUILD_DIR}/_build
	go test ${PKG}/...

lint: install-developer-tools
	go run _support/lint.go

package: build
	./_support/package/package ${CMDS}

notice:	${BUILD_DIR}/_build install-developer-tools
	cd ${PKG_BUILD_DIR} && govendor license -template _support/notice.template -o ${BUILD_DIR}/NOTICE

.PHONY: notice-up-to-date
notice-up-to-date: ${BUILD_DIR}/_build install-developer-tools
	@(cd ${PKG_BUILD_DIR} && govendor license -template _support/notice.template | cmp - NOTICE) || (echo >&2 "NOTICE requires update: 'make notice'" && false)

clean:	clean-build
	rm -rf internal/testhelper/testdata
	rm -f $(foreach cmd,${CMDS},./${cmd})

clean-build:
	rm -rf ${BUILD_DIR}/_build

.PHONY: format
format:
	@go run _support/gofmt-all.go -f

.PHONY: install-developer-tools
install-developer-tools:
	@go run _support/go-get-if-missing.go govendor github.com/kardianos/govendor
	@go run _support/go-get-if-missing.go golint github.com/golang/lint/golint
