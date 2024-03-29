.PHONY: clean vet test lint lint-ci

# default task invoked while running make
all: clean test

test: .build/sqlite3/sqlite3.c
	go test -v -tags="libsqlite3,sqlite_json1" ./...

vet: .build/sqlite3/sqlite3.c
	go vet -v -tags="libsqlite3,sqlite_json1" ./...

lint: .build/sqlite3/sqlite3.c
	golangci-lint run --build-tags libsqlite3,sqlite_json1

# target to download sqlite3 amalgamation code
.build/sqlite3/sqlite3.c:
	$(call log, $(CYAN), "downloading sqlite3 amalgamation source v3.35.0")
	$(eval SQLITE_DOWNLOAD_DIR = $(shell mktemp -d))
	@curl -sSLo $(SQLITE_DOWNLOAD_DIR)/sqlite3.zip https://www.sqlite.org/2021/sqlite-amalgamation-3350000.zip
	$(call log, $(GREEN), "downloaded sqlite3 amalgamation source v3.35.0")
	$(call log, $(CYAN), "unzipping to $(SQLITE_DOWNLOAD_DIR)")
	@(cd $(SQLITE_DOWNLOAD_DIR) && unzip sqlite3.zip > /dev/null)
	$(call log, $(CYAN), "moving to .build/sqlite3")
	@rm -rf .build/sqlite3 > /dev/null
	@mkdir -p .build/sqlite3
	@mv $(SQLITE_DOWNLOAD_DIR)/sqlite-amalgamation-3350000/* .build/sqlite3

clean:
	$(call log, $(YELLOW), "nuking .build/")
	@-rm -rf .build/

# ========================================
# some utility methods

# ASCII color codes that can be used with functions that output to stdout
RED		:= 1;31
GREEN	:= 1;32
ORANGE	:= 1;33
YELLOW	:= 1;33
BLUE	:= 1;34
PURPLE	:= 1;35
CYAN	:= 1;36

# log:
#	print out $2 to stdout using $1 as ASCII color codes
define log
	@printf "\033[$(strip $1)m-- %s\033[0m\n" $2
endef
