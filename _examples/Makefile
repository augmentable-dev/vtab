# compile all extensions under _examples/

.PHONY: all
all: helloworld.so

.PHONY: clean
clean:
	-rm -f helloworld.so

helloworld.so:
	go build -buildmode=c-shared -o $@ ./helloworld
