#!./../env/bin/python
from uuid import uuid1
import cStringIO
from PIL import Image

import itertools
import mimetools
import mimetypes
import time
import urllib2


class MultiPartForm(object):
    __slots__ = ['files', 'boundary']

    def __init__(self):
        self.files = []
        self.boundary = mimetools.choose_boundary()
        return

    def get_content_type(self):
        return 'multipart/form-data; boundary=%s' % self.boundary

    def add_file(self, fieldname, filename, fileHandle, mimetype=None):
        body = fileHandle.read()
        if mimetype is None:
            mimetype = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        self.files.append((fieldname, filename, mimetype, body))
        return

    def __str__(self):
        parts = []
        part_boundary = '--' + self.boundary

        parts.extend(
            [part_boundary,
             'Content-Disposition: file; name="%s"; filename="%s"' % \
             (field_name, filename),
             'Content-Type: %s' % content_type,
             '',
             body,
             ]
            for field_name, filename, content_type, body in self.files
        )

        flattened = list(itertools.chain(*parts))
        flattened.append('--' + self.boundary + '--')
        flattened.append('')
        return '\r\n'.join(flattened)


def send(url, filename, file, iteration=None):
    try:
        form = MultiPartForm()
        form.add_file('file', filename, file)
        body = str(form)
        request = urllib2.Request(url)
        request.add_header('X-Authentication', 'f9bf78b9a18ce6d46a0cd2b0b86df9da')
        request.add_header('User-agent', 'Mozilla/5.0')
        request.add_header('Content-type', form.get_content_type())
        request.add_header('Content-length', len(body))
        request.add_data(body)
        urllib2.urlopen(request)
    except Exception as e:
        print(e)
        if iteration is None:
            iteration = 0
        if iteration <= 5:
            iteration += 1
            send(url, filename, file, iteration)
        else:
            raise Exception(e)


if __name__ == '__main__':
    url = 'https://cdn.yottos.com/img2/f8/f8fc9b51e0cc11e7b2d2d8cb8a7f86f0.webp'
    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', 'Mozilla/5.0')]
    response = opener.open(url)
    f = cStringIO.StringIO(response.read())
    i = Image.open(f).convert('RGBA')
    png = cStringIO.StringIO()
    webp = cStringIO.StringIO()
    i.save(png, 'PNG')
    i.save(webp, 'WebP')
    count = 0
    start_time = time.time()
    for i in range(0, 10):
        count +=1

        png.seek(0)
        webp.seek(0)
        new_filename = uuid1().get_hex()
        send_png_url = 'http://cdn.api.srv-12.yottos.com/img3/%s/%s.png' % (new_filename[:2], new_filename)
        send_webp_url = 'http://cdn.api.srv-12.yottos.com/img3/%s/%s.webp' % (new_filename[:2], new_filename)
        send(send_png_url, '%s.png' % new_filename, png)
        send(send_webp_url, '%s.webp' % new_filename, webp)

    t = time.time() - start_time
    print('SERVER RESPONSE: --- %s seconds --- %s count --- %s per second' % (t, count, count/t))

