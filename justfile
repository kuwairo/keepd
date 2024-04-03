build:
	CGO_ENABLED=0 go build -o keepd

clean:
	rm -f keepd
