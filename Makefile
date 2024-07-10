master:
	cargo watch -q -c -w src/  -x "run"

r1:
	cargo watch -q -c -w src/  -x "run --  --port 6380 --replicaof 'localhost 6379'"

r2:
	cargo watch -q -c -w src/  -x "run --  --port 6381 --replicaof 'localhost 6379'"





