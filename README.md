# Simple SQS Proxy

To stop any lag in sending SQS push, we have a local listener that accepts
messages, buffers them and sends them on. Our web-app is then guarranteed low
latency sending messages to a queue.

Will use IAM Role Credentials if not in Environment (`AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`).

## Usage

Install dependencies: `npm install`

Configure in the `conf/config.json` to your liking:

    {
      "region": "eu-west-1",
      "amazon_id": "123456789",
      "port": 7890
    }

  - region: the AWS region for the queue (default: "eu-west-1")
  - amazon_id: your AWS account ID (required! no default)
  - port: the port the proxy will listen on (default: 7890)

Run the binary `bin/sqs-proxy` (symlink this to your path, e.g. `/usr/local/bin`).

Now you can send messages in the following format:

    <queue name>|<message>\n

or if you want to delay the message by a given number of seconds:
    
    <delay in seconds>|<queue name>|<message>\n
    
e.g. `"testq|I am a meesage"` followed by a linebreak.
or   `"30|testq|I am a meesage"` followed by a linebreak.

