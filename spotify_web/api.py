from flask import Flask

from service import WebService

app = Flask(__name__)
service = WebService()


@app.route('/')
def index():
    return 'Welcome to Spotify Web API'


@app.route('/track/<track_id>/plays_count')
def track_plays_count(track_id):
    if not track_id:
        raise ValueError('track_id is required')

    plays_count = service.get_track_plays_count_by_id(track_id=track_id)
    return {'track_id': track_id, 'plays_count': plays_count}, 200


if __name__ == '__main__':
    app.run(debug=True)
