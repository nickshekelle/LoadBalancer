loadbalancer:
	gcc loadbalancer.c -pthread -lm -o loadbalancer
clean:
	rm loadbalancer