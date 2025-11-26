#!/usr/bin/env python3
"""WebSocket relay: consumes fraud_predictions Kafka topic and broadcasts to websocket clients.

Lightweight relay built for development/testing. It listens to Kafka topic 'fraud_predictions'
and forwards JSON messages to any connected WebSocket clients.
"""
import asyncio
import json
import logging
import os
from kafka import KafkaConsumer
import websockets

LOG = logging.getLogger('relay')
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
LOG.addHandler(handler)

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
INPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'fraud_predictions')
WS_HOST = os.getenv('WS_HOST', '0.0.0.0')
WS_PORT = int(os.getenv('WS_PORT', '8000'))

connected = set()


async def ws_handler(websocket, path):
    LOG.info(f'Client connected: {websocket.remote_address}')
    connected.add(websocket)
    try:
        # Keep connection open -- this is a push-only client
        async for _ in websocket:
            pass
    except websockets.exceptions.ConnectionClosed:
        LOG.info(f'Client disconnected: {websocket.remote_address}')
    finally:
        connected.remove(websocket)


async def kafka_to_ws():
    LOG.info(f'Connecting to Kafka at {KAFKA_BOOTSTRAP}, topic={INPUT_TOPIC}')
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        auto_offset_reset='latest',
        group_id='relay_group',
        value_deserializer=lambda m: m.decode('utf-8') if m else None,
        consumer_timeout_ms=1000
    )

    while True:
        try:
            for message in consumer:
                if message.value is None:
                    continue
                msg_text = message.value
                try:
                    obj = json.loads(msg_text)
                except Exception:
                    obj = {'payload': msg_text}

                if connected:
                    payload = json.dumps(obj)
                    # broadcast concurrently
                    await asyncio.wait([ws.send(payload) for ws in connected])

            await asyncio.sleep(0.2)

        except Exception as e:
            LOG.exception('Kafka consumer error: %s', e)
            await asyncio.sleep(5)


async def main():
    LOG.info(f'Starting relay websocket server on {WS_HOST}:{WS_PORT}')
    server = await websockets.serve(ws_handler, WS_HOST, WS_PORT)

    kafka_task = asyncio.create_task(kafka_to_ws())

    await server.wait_closed()
    await kafka_task


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        LOG.info('Relay shutting down')
