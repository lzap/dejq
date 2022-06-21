.PHONY=build
build:
	go build

.PHONY=tools
tools:
	go install github.com/jackc/tern@latest

.PHONY=migrate
migrate:
	tern migrate -m postgres/jobqueue/migrations
