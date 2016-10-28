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
import test_utils.neontest

_log = logging.getLogger(__name__)

import utils.video_download as uvd


class TestFFmpegRotatorPP(test_utils.neontest.AsyncTestCase):

    def test_removes_rotate_metadata(self):
        mov_path = '/utils/test/rotated.mov'
        in_file = u'' + __base_path__ + mov_path
        info = {'filepath': in_file}
        mock_ydl = MagicMock()
        mock_ydl.params = {
            'ffmpeg_location': '/usr/bin/ffmpeg'}
        processor = uvd.FFmpegRotatorPP(mock_ydl)

        try:
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

        finally:
            # Always clean up the output file.
            if in_file != info['filepath']:
                os.remove(info['filepath'])


if __name__ == '__main__':
    unittest.main()
