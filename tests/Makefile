APPS = serialize1.exe serialize2.exe serialize3.exe serialize4.exe \
			 serialize5.exe ping-pong.exe prefix-drop-test.exe weather.exe \
			 weather-all-msgs.exe poll.exe
MORE =

all: $(APPS)

%.exe: %.sml
	../src/build/bin/mlton -verbose 2 -output $@ -const 'Exn.keepHistory true' -drop-pass 'deepFlatten' $(MORE) $<
clean:
	rm -f *~ *.o *.ssa *.rssa *.machine *.xml *.sxml *.ssa2 *.core-ml $(APPS)
