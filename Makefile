MODULE=order
SRCDIR=./src
DISTDIR=./dist
SERVER=$(DISTDIR)/$(MODULE)-server.js
PROCESSOR=$(DISTDIR)/$(MODULE)-processor.js
CRON=$(DISTDIR)/$(MODULE)-cron.js
TMPSERVER=$(DISTDIR)/server.js
TMPPROCESSOR=$(DISTDIR)/processor.js
TMPCRON=$(DISTDIR)/cron.js
NPM=npm

all: $(SERVER) $(PROCESSOR) $(CRON)

$(TMPSERVER) $(TMPPROCESSOR) $(TMPCRON): $(SRCDIR)/server.ts $(SRCDIR)/processor.ts $(SRCDIR)/cron.ts
	tsc || rm $(TMPSERVER) $(TMPPROCESSOR) $(TMPCRON)

$(SERVER): $(TMPSERVER)
	mv $< $@

$(PROCESSOR): $(TMPPROCESSOR)
	mv $< $@

$(CRON): $(TMPCRON)
	mv $< $@

$(SRCDIR)/server.ts: node_modules typings

$(SRCDIR)/processor.ts: node_modules typings

$(SRCDIR)/cron.ts: node_modules typings

node_modules:
	$(NPM) install

typings:
	typings install

clean:
	rm -rf $(DISTDIR)

.PHONY: all clean
