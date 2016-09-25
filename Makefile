DISTDIR=./dist
SRCDIR=./src
SERVER=$(DISTDIR)/server.js
PROCESSOR=$(DISTDIR)/processor.js
NPM=npm

NS = hive
VERSION ?= 1.0.0

REPO = order-service

all: $(SERVER) $(PROCESSOR)

$(SERVER) $(PROCESSOR): $(SRCDIR)/server.ts $(SRCDIR)/processor.ts
	tsc || rm $(SERVER) $(PROCESSOR)
	
$(SRCDIR)/server.ts: node_modules typings
$(SRCDIR)/processor.ts: node_modules typings

node_modules:
	$(NPM) install

typings:
	typings install

build: all
	docker build -t $(NS)/$(REPO):$(VERSION) .

rmi:
	docker rmi $(NS)/$(REPO):$(VERSION)

clean:
	rm -rf $(DISTDIR)

.PHONY: all build rmi clean
