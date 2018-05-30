fmt: dep
	go fmt .

unit: fmt
	go test . -v || exit;

dep: # install dep
ifndef DEP
	go get github.com/golang/dep
	dep ensure
endif