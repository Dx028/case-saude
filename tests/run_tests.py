"""Executa todos os testes automatizados (tests/test_*.py) numa única sessão Spark local."""
import os
import sys
import unittest

AQUI = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, AQUI)
resultado = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.discover(AQUI, pattern="test_*.py"))
sys.exit(0 if resultado.wasSuccessful() else 1)
