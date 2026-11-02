# Experiments (LLD §16): `make e1` … `make e9` build the images if the sources changed, run
# the experiment and plot it into runs/<UTC-ts>-eN/. Pass script options with ARGS:
#
#     make e1 ARGS="--repeat 1 --seed 7"

.PHONY: images e1 e2 e3 e4

images:
	docker build -q -f deploy/docker/Dockerfile.build -t elog-build .
	docker build -q -f deploy/docker/Dockerfile.runtime -t elog .

e1: images
	experiments/e1_steady_state.sh $(ARGS)

e2: images
	experiments/e2_consumer_kill.sh $(ARGS)

e3: images
	experiments/e3_session_sweep.sh $(ARGS)

e4: images
	experiments/e4_eager_vs_sticky.sh $(ARGS)
