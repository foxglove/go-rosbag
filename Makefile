test:
	$(MAKE) -C cli/r0sbag test
	$(MAKE) -C rosbag test

install-cli:
	$(MAKE) -C cli/r0sbag install
