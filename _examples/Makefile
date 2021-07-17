# compile all extensions under _examples/

.PHONY: clean

all: clean helloworld.so

clean:
	-rm -f helloworld.so

helloworld.so:
	go build -buildmode=c-shared -o $@ ./helloworld
