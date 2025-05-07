#!/bin/bash

rm -rf ../light-protocol/sdk-libs/photon-api

npx @openapitools/openapi-generator-cli generate \
  -i src/openapi/specs/api.yaml \
  -g rust \
  -o ../light-protocol/sdk-libs/photon-api \
  --additional-properties=preferUnsignedInt=true,packageName=photon-api
