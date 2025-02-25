runtests:
	scala test --js .
	scala test .

publishLocal:
	scala --power publish local . --organization io.github.bishabosha --name dataframe-nt --project-version $(shell cat .project-version)
	scala --power publish local . --js --organization io.github.bishabosha --name dataframe-nt --project-version $(shell cat .project-version)
