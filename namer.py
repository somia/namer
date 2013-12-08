#!/usr/bin/env python

from __future__ import absolute_import, print_function

import ConfigParser as configparser
import argparse
import logging
import os
import random
import sys
import tempfile
import time

import boto.dynamodb2
import boto.dynamodb2.table
import boto.utils

log_handler = logging.handlers.SysLogHandler(address="/dev/log")
log_handler.setFormatter(logging.Formatter("%(name)s: %(levelname)s: %(message)s"))
log = logging.getLogger("namer")
log.addHandler(log_handler)
log.setLevel(logging.DEBUG)

def main():
	parser = argparse.ArgumentParser()
	parser.add_argument("dynamodbtable")
	parser.add_argument("conffiles", nargs="+")
	args = parser.parse_args()

	identity = boto.utils.get_instance_identity()["document"]
	instance = identity["instanceId"]
	conn = boto.dynamodb2.connect_to_region(identity["region"])
	table = boto.dynamodb2.table.Table(args.dynamodbtable, connection=conn)

	config = configparser.SafeConfigParser()
	config.read(args.conffiles)

	retval = 0

	for section in config.sections():
		log.info("registering %s", section)

		while True:
			try:
				name = None
				names = set()
				for result in table.query(consistent=True, type__eq=section):
					n = int(result["name"])
					i = result.get("instance")
					if i and i == instance:
						name = n
						break
					names.add(n)

				if name is not None:
					log.info("%s %d already registered", section, name)
					break

				name = 1
				while name in names:
					name += 1

				if table.put_item(dict(type=section, name=name, instance=instance)):
					log.info("%s %d registered", section, name)
					break

				log.warning("%s registration conflict: %d", section, name)
			except Exception:
				log.exception(section)

			time.sleep(1 + random.random())
			log.info("retrying %s registration", section)

		ok = False

		for path, fmt in config.items(section):
			try:
				dirname = os.path.dirname(path)
				basename = os.path.basename(path)
				content = fmt.format(name)

				with tempfile.NamedTemporaryFile(prefix="." + basename, dir=dirname) as temp:
					print(content, file=temp)
					temp.flush()
					os.rename(temp.name, path)
					temp.delete = False
					ok = True
			except Exception:
				log.exception(path)

		if ok:
			print(section)
		else:
			log.warning("unregistering %s %d", section, name)
			table.delete_item(type=section, name=name)
			retval = 1

	return retval

if __name__ == "__main__":
	try:
		retval = main()
	except Exception:
		log.exception("failed")
		retval = 1

	sys.exit(retval)
