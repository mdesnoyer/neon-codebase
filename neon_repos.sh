# Determine if running from EC2 instance, if so, use a Region-local S3 bucket
# for Neon dependencies.
#
# To use: source neon_deps.sh
#

if AZ=$(curl -s 169.254.169.254/latest/meta-data/placement/availability-zone) ; then
  REGION=${AZ%?}
  export AZ REGION
fi

PROTO=https

case $REGION in 
  ( us-east-1 )
    export NEON_DEPS_URL="${PROTO}://s3.amazonaws.com/neon-dependencies-us-east-1/index.html"
  ;;
  ( * ) # use default in AWS Region: US-West-1
    export NEON_DEPS_URL="${PROTO}://s3-us-west-1.amazonaws.com/neon-dependencies/index.html"
  ;;
esac

echo "Pip index url: ${NEON_DEPS_URL}"
