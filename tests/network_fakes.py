"""Fakes of the host's network tools for the network faults' tests: ``iptables``/``iptables-restore`` and
``tc``/``ip``, recording every call and keeping the chains, devices, qdiscs and filters they make."""

import os
from types import SimpleNamespace

from valkey_fuzzer import hoststate
from valkey_fuzzer.components import network


class Iptables:
    """Records iptables calls; ``fail`` maps (verb, nth call of that verb) to an error text or an exception. A
    ``restore`` (``iptables-restore``'s stdin) runs its rule lines as calls, so the verbs count across both."""

    def __init__(self, fail=None):
        self.calls, self.fail, self.chains, self.jumps = [], dict(fail or {}), set(), []
        self.restores = []  # every iptables-restore stdin, as given

    def install(self, monkeypatch):
        """Route the network module's iptables and iptables-restore calls here."""

        monkeypatch.setattr(network, "run_iptables", self)
        monkeypatch.setattr(network, "run_iptables_restore", self.restore)
        return self

    def restore(self, text):
        """``iptables-restore -w -n``'s stdin: the filter table's rules between ``*filter`` and ``COMMIT``, run in
        order up to the first that fails, which the stderr names by line as the legacy tool does."""

        self.restores.append(text)
        lines = text.splitlines()
        assert text.endswith("\n") and lines[0] == "*filter" and lines[-1] == "COMMIT", text
        for number, line in enumerate(lines[1:-1], start=2):
            assert line.split()[0] in ("-A", "-I"), line
            result = self(*line.split())
            if result.returncode != 0:
                error = f"{result.stderr}\niptables-restore: line {number} failed"
                return SimpleNamespace(returncode=1, stdout="", stderr=error)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    def __call__(self, *args):
        self.calls.append(args)
        verb, ok = args[0], SimpleNamespace(returncode=0, stdout="", stderr="")
        error = self.fail.get((verb, sum(1 for call in self.calls if call[0] == verb)))
        if isinstance(error, BaseException):
            raise error
        if error is not None:
            return SimpleNamespace(returncode=1, stdout="", stderr=error)
        if verb in ("-S", "-C"):
            ok.returncode = 0 if args[-1] in (self.chains if verb == "-S" else self.jumps) else 1
        elif verb in ("-N", "-X"):
            (self.chains.add if verb == "-N" else self.chains.discard)(args[1])
        elif verb in ("-I", "-D"):
            (self.jumps.append if verb == "-I" else self.jumps.remove)(args[-1])
        return ok


class Tc:
    """Records ``tc`` and ``ip`` calls and keeps the devices, lo's ingress qdisc, and the filters they make.

    ``fail`` maps (``"tc filter add"``, nth call of that verb) to an error text or an exception, as ``Iptables``
    does; ``ingress`` is whether lo already has an ingress qdisc; ``taken`` are prefs another run's filters hold.
    """

    def __init__(self, fail=None, *, ingress=False, taken=()):
        self.calls, self.fail, self.ingress = [], dict(fail or {}), ingress
        self.devices: dict = {}  # name -> its netem arguments, once a qdisc is on it
        self.filters: dict = {pref: [(1, "10.0.0.1", "10.0.0.2", "other0")] for pref in taken}
        self.batches = []  # every tc -batch stdin, as given

    def tc(self, *args):
        return self("tc", *args)

    def tc_batch(self, text):
        """``tc -force -batch -``'s stdin: one tc command per line, each run as a call and every line tried, as
        ``-force`` does; the stderr names each failed line after its own error, as the real tool's does."""

        self.batches.append(text)
        assert text.endswith("\n"), text
        stderr = ""
        for number, line in enumerate(text.splitlines(), start=1):
            result = self.tc(*line.split())
            if result.returncode != 0:
                stderr += f"{result.stderr}\nCommand failed -:{number}\n"
        return self._reply(1 if stderr else 0, stderr=stderr)

    def ip(self, *args):
        return self("ip", *args)

    def __call__(self, tool, *args):
        self.calls.append((tool, *args))
        words = [word for word in args if not word.startswith("-")]
        verb = " ".join((tool, *words[:2]))
        error = self.fail.get((verb, sum(1 for call in self.calls if call[0] == tool and call[1:3] == args[:2])))
        if isinstance(error, BaseException):
            raise error
        if error is not None:
            return self._reply(1, stderr=error)
        return getattr(self, "_" + verb.replace(" ", "_"))(words[2:])

    @staticmethod
    def _reply(returncode=0, stdout="", stderr=""):
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    def _ip_link_add(self, words):  # ip link add dev NAME type ifb
        name = words[words.index("dev") + 1]
        if name in self.devices:
            return self._reply(2, stderr="RTNETLINK answers: File exists")
        self.devices[name] = None
        return self._reply()

    def _ip_link_set(self, words):
        return self._reply() if words[1] in self.devices else self._reply(1, stderr=f'Cannot find device "{words[1]}"')

    def _ip_link_del(self, words):
        if words[1] not in self.devices:
            return self._reply(1, stderr=f'Cannot find device "{words[1]}"')
        del self.devices[words[1]]
        return self._reply()

    def _ip_link_show(self, words):  # ip -o link show type ifb
        lines = [f"{n + 3}: {name}: <BROADCAST,NOARP,UP,LOWER_UP> mtu 1500 qdisc netem state UNKNOWN mode DEFAULT"
                 for n, name in enumerate(self.devices)]  # fmt: skip
        return self._reply(stdout="\n".join(lines) + ("\n" if lines else ""))

    def _tc_qdisc_add(self, words):  # tc qdisc add dev NAME root netem ...  |  tc qdisc add dev lo ingress
        name = words[words.index("dev") + 1]
        if name == "lo":
            assert words[-1] == "ingress", words
            if self.ingress:
                return self._reply(2, stderr="Error: Exclusivity flag on, cannot modify.")
            self.ingress = True
            return self._reply()
        assert name in self.devices and words[words.index("root") + 1] == "netem", words
        self.devices[name] = words[words.index("netem") + 1 :]
        return self._reply()

    def _tc_qdisc_del(self, words):  # tc qdisc del dev lo ingress
        assert words[words.index("dev") + 1] == "lo", words
        if not self.ingress:
            return self._reply(2, stderr="Error: Cannot find specified qdisc on specified device.")
        assert not self.filters, "lo's ingress qdisc was deleted with filters on it"
        self.ingress = False
        return self._reply()

    def _tc_qdisc_show(self, words):
        return self._reply(
            stdout="qdisc noqueue 0: root refcnt 2\n"
            + ("qdisc ingress ffff: parent ffff:fff1\n" if self.ingress else "")
        )

    def _tc_filter_add(self, words):
        assert words[:4] == ["dev", "lo", "ingress", "protocol"] and words[4] == "ip", words
        if not self.ingress:
            return self._reply(2, stderr="Error: Parent Qdisc doesn't exists.")
        pref, handle = int(words[words.index("pref") + 1]), int(words[words.index("handle") + 1])
        if any(held == handle for held, *_ in self.filters.get(pref, ())):
            return self._reply(2, stderr="RTNETLINK answers: File exists")
        assert words[words.index("flower") + 1 : words.index("action")] == [
            "ip_proto", "tcp", "src_ip", words[words.index("src_ip") + 1], "dst_ip", words[words.index("dst_ip") + 1]
        ], words  # fmt: skip
        assert words[words.index("action") + 1 :][:4] == ["mirred", "egress", "redirect", "dev"], words
        device = words[-1]
        assert device in self.devices, words
        self.filters.setdefault(pref, []).append(
            (handle, words[words.index("src_ip") + 1], words[words.index("dst_ip") + 1], device)
        )
        return self._reply()

    def _tc_filter_del(self, words):  # tc filter del dev lo ingress protocol ip pref P
        pref = int(words[words.index("pref") + 1])
        if pref not in self.filters:
            return self._reply(2, stderr="Error: Filter with specified priority/protocol not found.")
        del self.filters[pref]
        return self._reply()

    def _tc_filter_show(self, words):
        lines = []
        for pref, filters in sorted(self.filters.items()):
            lines.append(f"filter protocol ip pref {pref} flower chain 0 ")
            for handle, source, destination, device in filters:
                lines += [
                    f"filter protocol ip pref {pref} flower chain 0 handle 0x{handle:x} ",
                    "  eth_type ipv4", "  ip_proto tcp", f"  src_ip {source}", f"  dst_ip {destination}",
                    "  not_in_hw",
                    f"\taction order 1: mirred (Egress Redirect to device {device}) stolen",
                    " \tindex 1 ref 1 bind 1",
                ]  # fmt: skip
        return self._reply(stdout="\n".join(lines) + ("\n" if lines else ""))

    def install(self, monkeypatch, tmp_path):
        """Route the network module's tc and ip calls here, with the lock and the ingress marker in a directory
        beside ``tmp_path`` instead of the host's shared directory."""

        monkeypatch.setattr(network, "run_tc", self.tc)
        monkeypatch.setattr(network, "run_tc_batch", self.tc_batch)
        monkeypatch.setattr(network, "run_ip", self.ip)
        monkeypatch.setattr(hoststate, "default_root", lambda: str(tmp_path.parent / f"{tmp_path.name}-host"))
        if self.ingress:
            # Not the fuzzer's: no marker says it added the qdisc, so no fuzzer process removes it.
            assert not os.path.exists(hoststate._lo_ingress_marker())
        return self

    @property
    def marked(self):
        """Whether the marker says the fuzzer added lo's ingress qdisc."""

        return os.path.exists(hoststate._lo_ingress_marker())

    def flows(self):
        """``(source, destination, device)`` of every filter, in pref and handle order."""

        return [
            (source, destination, device)
            for pref in sorted(self.filters)
            for _, source, destination, device in sorted(self.filters[pref])
        ]
