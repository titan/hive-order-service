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

$(SRCDIR)/server.ts: node_modules

$(SRCDIR)/processor.ts: node_modules

$(SRCDIR)/cron.ts: node_modules

node_modules:
	$(NPM) install

clean:
	rm -rf $(DISTDIR)

.PHONY: all clean
