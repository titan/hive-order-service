MODULE=order
SRCDIR=./src
DISTDIR=./dist
SERVER=$(DISTDIR)/$(MODULE)-server.js
PROCESSOR=$(DISTDIR)/$(MODULE)-processor.js
TMPSERVER=$(DISTDIR)/server.js
TMPPROCESSOR=$(DISTDIR)/processor.js
NPM=npm

all: $(SERVER) $(PROCESSOR)

$(TMPSERVER) $(TMPPROCESSOR): $(SRCDIR)/server.ts $(SRCDIR)/processor.ts
	tsc || rm $(TMPSERVER) $(TMPPROCESSOR)

$(SERVER): $(TMPSERVER)
	mv $< $@

$(PROCESSOR): $(TMPPROCESSOR)
	mv $< $@

$(SRCDIR)/server.ts: node_modules typings
$(SRCDIR)/processor.ts: node_modules typings

node_modules:
	$(NPM) install

typings:
	typings install

clean:
	rm -rf $(DISTDIR)

.PHONY: all clean
