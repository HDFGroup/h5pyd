##############################################################################
# Copyright by The HDF Group.                                                #
# All rights reserved.                                                       #
#                                                                            #
# This file is part of H5Serv (HDF5 REST Server) Service, Libraries and      #
# Utilities.  The full HDF5 REST Server copyright notice, including          #
# terms governing use, modification, and redistribution, is contained in     #
# the file COPYING, which can be found at the root of the source code        #
# distribution tree.  If you do not have access to this file, you may        #
# request a copy from help@hdfgroup.org.                                     #
##############################################################################
""" Getting a password into the command line tools without putting it in argv, and keeping
hsconfigure from echoing it """

import contextlib
import io
import os
import unittest
from unittest import mock

from stubs import scratch_home

from h5pyd._apps import hsconfigure
from h5pyd._apps.config import Config as AppConfig


class FakeStdin(io.StringIO):
    def __init__(self, text="", tty=False):
        super().__init__(text)
        self._tty = tty

    def isatty(self):
        return self._tty


def no_getpass(prompt=""):
    raise AssertionError("getpass should not be called")


class TestPasswordInput(unittest.TestCase):

    def _parse(self, args, stdin, getpass=no_getpass):
        cfg = AppConfig()
        with mock.patch("sys.stdin", stdin), mock.patch("getpass.getpass", side_effect=getpass) as gp:
            domains = cfg.set_cmd_flags(args)
        return cfg, domains, gp

    def test_password_stdin(self):
        with scratch_home():
            cfg, domains, _ = self._parse(["-u", "bob", "--password-stdin", "/home/bob/x.h5"],
                                          FakeStdin("s3cret pass\nnext line\n"))
            self.assertEqual(cfg["hs_password"], "s3cret pass")
            self.assertEqual(domains, ["/home/bob/x.h5"])

    def test_no_prompt_without_tty(self):
        with scratch_home():
            cfg, _, _ = self._parse(["-u", "bob", "/home/bob/x.h5"], FakeStdin(tty=False))
            self.assertIn(cfg["hs_password"], (None, ""))

    def test_prompt_with_tty(self):
        with scratch_home():
            cfg, _, gp = self._parse(["-u", "bob", "/home/bob/x.h5"], FakeStdin(tty=True),
                                     getpass=lambda prompt="": "typed")
            self.assertEqual(gp.call_count, 1)
            self.assertEqual(cfg["hs_password"], "typed")

    def test_no_prompt_when_password_set(self):
        with scratch_home():
            os.environ["HS_PASSWORD"] = "from-env"
            cfg, _, _ = self._parse(["-u", "bob", "/home/bob/x.h5"], FakeStdin(tty=True))
            self.assertEqual(cfg["hs_password"], "from-env")
            cfg, _, _ = self._parse(["-u", "bob", "-p", "flag", "/home/bob/x.h5"], FakeStdin(tty=True))
            self.assertEqual(cfg["hs_password"], "flag")

    def test_password_stdin_not_from_environment(self):
        with scratch_home():
            os.environ["PASSWORD_STDIN"] = "0"
            stdin = FakeStdin("not a password\n")
            cfg, _, _ = self._parse(["-u", "bob", "/home/bob/x.h5"], stdin)
            self.assertIn(cfg["hs_password"], (None, ""))
            self.assertEqual(stdin.read(), "not a password\n")  # stdin left alone

    def test_no_prompt_without_user_flag_or_with_api_key(self):
        with scratch_home():
            self._parse(["/home/bob/x.h5"], FakeStdin(tty=True))
            self._parse(["-u", "bob", "--api_key", "KEY", "/home/bob/x.h5"], FakeStdin(tty=True))


class TestHsconfigure(unittest.TestCase):

    def _run(self, answers, tty):
        """ run hsconfigure.main with the given answers to its prompts """
        answers = list(answers)
        stdout = io.StringIO()

        def answer(prompt=""):
            stdout.write(prompt)
            return answers.pop(0)

        with mock.patch("sys.stdin", FakeStdin(tty=tty)), \
                mock.patch.object(hsconfigure, "get_input", side_effect=answer), \
                mock.patch("getpass.getpass", side_effect=answer) as gp, \
                mock.patch.object(hsconfigure, "pingServer", return_value=True), \
                contextlib.redirect_stdout(stdout):
            hsconfigure.main()
        return stdout.getvalue(), gp.call_count

    def test_password_not_echoed(self):
        # endpoint, username, password, api key, save
        for tty in (True, False):
            with scratch_home() as (home, cwd):
                out, getpass_calls = self._run(
                    ["https://hsds.example.org", "bob", "TopSecret1", "", "Y"], tty)
                self.assertNotIn("TopSecret1", out)
                self.assertEqual(getpass_calls, 2 if tty else 0)
                with open(os.path.join(home, ".hscfg")) as f:
                    self.assertIn("hs_password = TopSecret1\n", f.read())

            # the saved password is shown masked as the default next time
            with scratch_home() as (home, cwd):
                with open(os.path.join(home, ".hscfg"), "w") as f:
                    f.write("hs_endpoint = https://hsds.example.org\nhs_username = bob\n")
                    f.write("hs_password = TopSecret1\nhs_api_key = APIKEY99\n")
                os.chmod(os.path.join(home, ".hscfg"), 0o600)
                out, _ = self._run(["", "", "", "", "Y"], tty)
                self.assertNotIn("TopSecret1", out)
                self.assertNotIn("APIKEY99", out)
                self.assertIn("Password [****]", out)


class TestHsconfigureFiles(unittest.TestCase):

    def test_reports_where_changes_go(self):
        # a ./.hscfg supplies the defaults, but changes are saved to ~/.hscfg
        with scratch_home() as (home, cwd):
            with open(os.path.join(cwd, ".hscfg"), "w") as f:
                f.write("hs_endpoint = https://hsds.example.org\n")
            stdout = io.StringIO()
            with mock.patch("sys.stdin", FakeStdin(tty=False)), \
                    mock.patch.object(hsconfigure, "get_input", side_effect=["", "", "", "", "Y"]), \
                    mock.patch.object(hsconfigure, "pingServer", return_value=True), \
                    contextlib.redirect_stdout(stdout):
                hsconfigure.main()
        out = stdout.getvalue()
        self.assertIn("Reading settings from:", out)
        self.assertIn(f"Changes are saved to: {os.path.join(home, '.hscfg')}", out)


if __name__ == "__main__":
    unittest.main()
