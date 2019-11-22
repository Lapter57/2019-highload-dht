from argparse import ArgumentParser
import abc
import random
import os
import binascii

class AmmoGenerator:
    def __init__(self, num_ammo, ammo_file, with_saving_keys=True, keys_file=None):
        self.num_ammo = num_ammo
        self.ammo_file = ammo_file
        self.keys_file = keys_file
        self.with_saving_keys = with_saving_keys
        if keys_file is not None and os.path.isfile(keys_file):
            with open(keys_file) as f:
                self.keys = f.read().splitlines()
                self.keys.reverse()
        else:
            self.keys = []

    def make_ammos(self):
        requests = []
        for i in range(self.num_ammo):
            tag, method, key, body = self.get_info()
            if self.with_saving_keys:
                self.keys.append(key)
            req = self.make_ammo(method, "/v0/entity?id=" + str(key), tag, body)
            requests.append(req)
        with open(self.ammo_file + ".ammo", 'wb') as f:
            for req in requests:
                f.write(req.encode("ascii"))
        if self.with_saving_keys:
            self.save_keys()

    @abc.abstractmethod
    def get_info(self):
        pass

    def save_keys(self):
        with open(self.ammo_file + ".keys", 'w') as f:
            for key in self.keys:
                f.write("%s\n" % key)

    def make_ammo(self, method, url, tag, body=None):
        req_with_body_template = (
            "{method} {url} HTTP/1.1\r\n"
            "Content-Length: {body_length}\r\n"
            "\r\n"
            "{body}"
        )
        req_without_body_template = (
            "{method} {url} HTTP/1.1\r\n\r\n"
        )
        req_template = ("{size} {tag}\n{request}\r\n")

        request = None
        if body is not None:
            request = req_with_body_template.format(
                method=method, url=url, body=body, body_length=len(body)
            )
        else:
            request = req_without_body_template.format(
                method=method, url=url
            )
        return req_template.format(size=len(request), tag=tag, request=request)

    def random_key(self):
        return hex(random.getrandbits(64)).rstrip("L").lstrip("0x")

    def random_body(self):
        return binascii.hexlify(os.urandom(128)).decode()

    def flip(self, prob):
        return True if random.random() < prob else False

class PutGenerator(AmmoGenerator):
    def __init__(self, num_ammo, prob=0.0):
        AmmoGenerator.__init__(self, num_ammo, "put")
        self.prob = prob

    def need_rewrite(self):
        return self.flip(self.prob) and len(self.keys) != 0

    def get_info(self):
        key = random.choice(self.keys) if self.need_rewrite() else self.random_key()
        return "put", "PUT", key, self.random_body()

class GetGenerator(AmmoGenerator):
    def __init__(self, num_ammo, keys_file, uneven_distr=False):
        AmmoGenerator.__init__(self, num_ammo, "get", False, keys_file)
        self.uneven_distr = uneven_distr

    def pick(self):
        return self.keys[int(round(random.expovariate(1/10))) % len(self.keys)]

    def get_info(self):
        key = self.pick() if self.uneven_distr else random.choice(self.keys)
        return "get", "GET", key, None

class MixGenerator(AmmoGenerator):
    def __init__(self, num_ammo, keys_file):
         AmmoGenerator.__init__(self, num_ammo, "mix", False, keys_file)

    def get_method(self):
        return "PUT" if self.flip(0.5) else "GET"

    def get_info(self):
        method = self.get_method()
        key = self.random_key() if method == "PUT" else random.choice(self.keys)
        body = self.random_body() if method == "PUT" else None
        return method.lower(), method, key, body

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
parser.add_argument('--num', action="store", type=int, dest='num', default=1000)
parser.add_argument('--keys', action="store", dest='keys')
args = parser.parse_args()

ammo_generator(args).make_ammos()
