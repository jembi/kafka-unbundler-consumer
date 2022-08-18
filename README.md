# kafka-unbundler-consumer
This is the base package for the kafka unbundler used in the jembi platform

# For dev purposes: 
1- Make your changes in index.ts file
2- Run: `yarn bundle` to create index.js file
3- Build the image docker (with specifying the image tag): `yarn build` or `docker build -t jembi/kafka-unbundler-consumer:<TAG_IMAGE> .`
4- Build the platform image

# Tests
To run unit tests: `yarn test`
