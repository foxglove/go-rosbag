test:
	$(MAKE) -C cli/r0sbag test
	$(MAKE) -C rosbag test

lint:
	$(MAKE) -C cli/r0sbag lint
	$(MAKE) -C rosbag lint


install-cli:
	$(MAKE) -C cli/r0sbag install
