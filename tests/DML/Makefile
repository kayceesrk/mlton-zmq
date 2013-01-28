APPS = ping-pong.app multi-consumer.app

MORE =

all: $(APPS)

%.exe: %.sml
	../../src/build/bin/mlton -verbose 2 -output $@ -const 'Exn.keepHistory true' -drop-pass 'deepFlatten' $(MORE) $<

%.app: %.mlb
	../../src/build/bin/mlton -verbose 2 -output $@ -const 'Exn.keepHistory true' -drop-pass 'deepFlatten' $(MORE) $<

clean:
	rm -f *~ *.o *.ssa *.rssa *.machine *.xml *.sxml *.ssa2 *.core-ml *.exe *.app
