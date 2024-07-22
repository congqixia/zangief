INSTALL_PATH := $(PWD)/bin

all: zangief

zangief:
	@echo "Compiling zangief"
	@mkdir -p $(INSTALL_PATH)
	@CGO_ENABLED=0 go build -o bin/zangief main.go