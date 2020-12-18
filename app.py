import os
import time

import redis
from flask import Flask, request, abort, jsonify, redirect

app = Flask(__name__)
db = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)


SNOWCLOUD_KEY = os.getenv("SNOWCLOUD_KEY")

TIME_TO_LIVE = 60

if not db.exists("snowcloud:id:pool"):
    for i in range(2**10):
        db.zadd("snowcloud:id:pool", {i: 0})


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "GET":
        return redirect("https://breq.dev/apps/snowflake")

    user = request.args.get("user")
    key = request.args.get("key")

    if not user:
        return abort(403)
    if not key:
        return abort(403)

    if key != SNOWCLOUD_KEY:
        return abort(403)

    renew = request.args.get("renew")

    if renew:
        last_expires = db.zscore("snowcloud:id:pool", renew)
        if not last_expires:
            return abort(403)  # invalid ID

        last_user = db.get(f"snowcloud:id:user:{renew}")
        if last_user != user:
            return abort(403)  # Not used by the same user

        worker_id = renew

    else:
        worker_id = db.zrange("snowcloud:id:pool", 0, 0)[0]

        last_expires = db.zscore("snowcloud:id:pool", worker_id)
        if float(last_expires) > time.time():
            return abort(500)  # no IDs available

        db.set(f"snowcloud:id:user:{worker_id}", user)

    expires = time.time() + TIME_TO_LIVE
    db.zadd("snowcloud:id:pool", {worker_id: expires})

    return jsonify({
        "worker_id": int(worker_id),
        "expires": float(expires),
        "ttl": TIME_TO_LIVE
    })


if __name__ == "__main__":
    app.run("0.0.0.0", port=5000)
