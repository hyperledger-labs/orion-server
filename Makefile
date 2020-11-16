.PHONY: test

test:
	scripts/run-unit-tests.sh

.PHONY: protos
protos: 
	docker run -it -v `pwd`:`pwd` -w `pwd` sykesm/fabric-protos:0.2 scripts/compile_go_protos.sh
