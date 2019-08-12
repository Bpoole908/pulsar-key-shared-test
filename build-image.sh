#!/bin/sh

suffix=$1
shift
case "$suffix" in
	java)
	;;

	python)
	;;

	""|"*")
	echo "You have to specify which image to build - 'java' or 'python' (no quotes)"
	echo "You provided '$suffix'"
	echo "Optionally you can specify the image name as second argument; 
		  default is twitter-demo-<suffix>:latest"
	exit 1
	;;
esac
IMAGE=${1:-key-share-test-$suffix}
docker build -f images/Dockerfile-$suffix -t $IMAGE .
