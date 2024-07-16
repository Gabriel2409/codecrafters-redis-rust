help:
	@echo "Available targets:"
	@echo "  - master: Runs the master redis server"
	@echo "  - r1: Runs the first replica" 
	@echo "  - r2: Runs the second replica" 
	@echo "  - rr1: Runs the replica of the first replica" 

master:
	cargo watch -q -c -w src/  -x "run"

master_rdb:
	cargo watch -q -c -w src/  -x "run -- --dir . --dbfilename test_dump.rdb"

r1:
	cargo watch -q -c -w src/  -x "run --  --port 6380 --replicaof 'localhost 6379'"

r2:
	cargo watch -q -c -w src/  -x "run --  --port 6381 --replicaof 'localhost 6379'"

rr1:
	cargo watch -q -c -w src/  -x "run --  --port 6382 --replicaof 'localhost 6380'"





