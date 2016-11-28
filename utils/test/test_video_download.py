#!/usr/bin/env python
import os
import os.path
import sys
__base_path__ = os.path.abspath(os.path.join(os.path.dirname(__file__), '..',
                                         '..'))
if sys.path[0] != __base_path__:
    sys.path.insert(0, __base_path__)

from mock import MagicMock
import logging
import subprocess
import tempfile
import test_utils.neontest

_log = logging.getLogger(__name__)

import utils.video_download as uvd


class TestFFmpegRotatorPP(test_utils.neontest.AsyncTestCase):

    @staticmethod
    def get_ffmpeg_path():
        path = subprocess.Popen(
            ['/usr/bin/which', 'ffmpeg'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE).communicate()[0]
        if path:
            return path.rstrip().decode('utf-8')

    def test_removes_rotate_metadata(self):
        mov_path = '/utils/test/rotated.mov'
        in_file = (__base_path__ + mov_path).decode('utf-8')
        info = {'filepath': in_file}
        mock_ydl = MagicMock()
        mock_ydl.params = {
            'ffmpeg_location': TestFFmpegRotatorPP.get_ffmpeg_path()}
        with tempfile.NamedTemporaryFile(suffix='.mp4') as out_file:

            processor = uvd.FFmpegRotatorPP(mock_ydl, out_file.name)
            paths, info = processor.run(info)

            # Assert output video's rotation metadata is removed.
            output = subprocess.check_output([
                    'ffprobe',
                    '-v',
                    'quiet',
                    '-show_streams',
                    info['filepath']],
                stderr=subprocess.STDOUT)
            self.assertEqual(-1, output.find('rotation'))

        # Put the mov back.
        if os.path.exists(out_file.name):
            shutil.move(out_file.name, in_file)

if __name__ == '__main__':
    unittest.main()
