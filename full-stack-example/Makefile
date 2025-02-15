packageJs:
	rm -rf ./out
	mkdir -p ./out
	scala --power package --js webpage -o ./out/webpage.js
	cp ./out/webpage.js ./assets/webpage.js

serveStatic:
	static-web-server -p 8081 -d .
