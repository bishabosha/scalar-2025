clean:
	rm -rf out

packageWeb:
	mkdir -p out/resources/assets
	cp -r ./assets/* ./out/resources/assets
	cp index.html ./out/resources/index.html

packageJs: packageWeb
	mkdir -p out/resources/assets
	scala --power package --js -f frontend -o out/resources/assets/webpage.js

serve:
	scala run backend --resource-dir ./out/resources
