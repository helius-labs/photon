#!/bin/bash

rm -rf ../light-protocol/photon-api
 
npx @openapitools/openapi-generator-cli generate \
  -i src/openapi/specs/api.yaml \
  -g rust \
  -o ../light-protocol/photon-api \
  --additional-properties=preferUnsignedInt=true,packageName=photon-api
