#!/bin/bash
LAMBDA_FUNC="ses-mail"
zip -rq lambda.zip . -x *__pycache__* 2zip.sh env.sh
aws lambda update-function-code --function-name $LAMBDA_FUNC --zip-file fileb://lambda.zip > /dev/null 2>&1
rm lambda.zip

