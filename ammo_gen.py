from argparse import ArgumentParser
import abc
import random
import io
import os

class AmmoGenerator:
    def __init__(self, num_ammo, tag, keys_file=None):
        self.num_ammo = num_ammo
        self.ammo_file = "ammo_" + tag
        self.keys_file = keys_file
        self.tag = tag
        if keys_file is not None and os.path.isfile(keys_file):
            with open(keys_file) as f:
                self.keys = set(f.read().splitlines())
                self.keys = list(self.keys)
        else:
            self.keys = []

    def make_ammos(self):
        with open(self.ammo_file, 'wb') as f:
            for i in range(self.num_ammo):
                method, key, body = self.get_info()
                if key not in self.keys:
                    self.keys.insert(0, key)
                self.make_ammo(f, method, "/v0/entity?id=" + str(key), self.tag, body)
        self.save_keys()

    @abc.abstractmethod
    def get_info(self):
        pass

    def save_keys(self):
        with open('keys_' + self.tag, 'w') as f:
            for key in self.keys:
                f.write("%s\n" % key)

    @staticmethod
    def make_ammo(file, method, url, tag, body=None):
        req_template = ( "%s %s HTTP/1.1\r\n" )
        content_length_template = ( "Content-Length: %d\r\n\r\n" )
        size_req_template = ( "%d %s\n" )

        binary_stream = io.BytesIO()
        req = req_template % (method, url)
        if body is not None:
            content_length = content_length_template % len(body)
            req_len = len(req) + len(content_length) + len(body) + len("\r\n")
            size_req = size_req_template % (req_len, tag)
            binary_stream.write(size_req.encode("ascii"))
            binary_stream.write(req.encode("ascii"))
            binary_stream.write(content_length.encode("ascii"))
            binary_stream.write(body)
        else:
            req_len = len(req) + len("\r\n")
            size_req = size_req_template % (req_len, tag)
            binary_stream.write(size_req.encode("ascii"))
            binary_stream.write(req.encode("ascii"))
        binary_stream.write("\r\n".encode("ascii"))
        binary_stream.seek(0)
        file.write(binary_stream.read())
        binary_stream.close()

    @staticmethod
    def random_key():
        return hex(random.getrandbits(64)).rstrip("L").lstrip("0x")

    @staticmethod
    def random_body():
        return os.urandom(256)

    @staticmethod
    def flip(prob):
        return True if random.random() < prob else False

class PutGenerator(AmmoGenerator):
    def __init__(self, num_ammo, prob=0.0):
        AmmoGenerator.__init__(self, num_ammo, "put")
        self.prob = prob

    def need_rewrite(self):
        return self.flip(self.prob) and len(self.keys) != 0

    def get_info(self):
        key = random.choice(self.keys) if self.need_rewrite() else self.random_key()
        return "PUT", key, self.random_body()

class GetGenerator(AmmoGenerator):
    def __init__(self, num_ammo, keys_file, uneven_distr=False):
        AmmoGenerator.__init__(self, num_ammo, "get", keys_file)
        self.uneven_distr = uneven_distr

    def pick(self):
        return self.keys[int(round(random.expovariate(1/10))) % len(self.keys)]

    def get_info(self):
        key = self.pick() if self.uneven_distr else random.choice(self.keys)
        return "GET", key, None

class MixGenerator(AmmoGenerator):
    def __init__(self, num_ammo, keys_file):
         AmmoGenerator.__init__(self, num_ammo, "mix", keys_file)

    def get_method(self):
        return "PUT" if self.flip(0.5) else "GET"

    def get_info(self):
        method = self.get_method()
        key = self.random_key() if method == "PUT" else random.choice(self.keys)
        body = self.random_body() if method == "PUT" else None
        return method, key, body

def ammo_generator(args):
    switcher = {
        'put': lambda: PutGenerator(args.num),
        'put_rew': lambda: PutGenerator(args.num, 0.1),
        'get': lambda: GetGenerator(args.num, args.keys),
        'get_un': lambda: GetGenerator(args.num, args.keys, True),
        'mix': lambda: MixGenerator(args.num, args.keys),
    }
    return switcher.get(args.method)()

methods=['put', 'put_rew', 'get', 'get_un', 'mix']
parser = ArgumentParser()
parser.add_argument('--method', choices=methods, action="store", dest='method', default="get")
parser.add_argument('--num', action="store", type=int, dest='num', default=1000000)
parser.add_argument('--keys', action="store", dest='keys')
args = parser.parse_args()

ammo_generator(args).make_ammos()
