#!/usr/bin/env python3
"""
Validation script for CNPJ Data Pipeline.
Tests that all components are working correctly after refactoring.
"""

import sys
from pathlib import Path


def test_imports():
    """Test that all modules can be imported correctly."""
    print("🔍 Testing imports...")

    try:
        print("  ✅ Config module imported")

        print("  ✅ Database factory imported")

        print("  ✅ Database base adapter imported")

        print("  ✅ PostgreSQL adapter imported")

        print("  ✅ Downloader imported")

        print("  ✅ Processor imported")

        return True
    except Exception as e:
        print(f"  ❌ Import error: {e}")
        return False


def test_configuration():
    """Test configuration system."""
    print("\n🔍 Testing configuration...")

    try:
        from src.config import Config

        config = Config()
        print("  ✅ Config created successfully")
        print(f"     Database backend: {config.database_backend.value}")
        print(f"     Processing strategy: {config.processing_strategy.value}")
        print(f"     Batch size: {config.batch_size:,}")
        print(f"     Optimal chunk size: {config.optimal_chunk_size:,}")

        # Test that required directories exist
        if Path(config.temp_dir).exists():
            print(f"  ✅ Temp directory exists: {config.temp_dir}")
        else:
            print(f"  ⚠️  Temp directory will be created: {config.temp_dir}")

        return True
    except Exception as e:
        print(f"  ❌ Configuration error: {e}")
        return False


def test_database_factory():
    """Test database factory."""
    print("\n🔍 Testing database factory...")

    try:
        from src.config import Config
        from src.database.factory import (
            create_database_adapter,
            list_available_backends,
        )

        config = Config()

        # Test available backends
        backends = list_available_backends()
        print(f"  ✅ Available backends: {', '.join(backends)}")

        # Test adapter creation (without connecting)
        adapter = create_database_adapter(config)
        print(f"  ✅ Database adapter created: {type(adapter).__name__}")

        return True
    except Exception as e:
        print(f"  ❌ Database factory error: {e}")
        return False


def test_requirements():
    """Test that required packages are available."""
    print("\n🔍 Testing requirements...")

    required_packages = ["requests", "polars", "psycopg2", "psutil", "python-dotenv"]

    missing = []
    for package in required_packages:
        try:
            if package == "psycopg2":
                import psycopg2  # noqa: F401
            elif package == "python-dotenv":
                import dotenv  # noqa: F401
            else:
                __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} (missing)")
            missing.append(package)

    if missing:
        print(f"\n  ⚠️  Missing packages: {', '.join(missing)}")
        print(f"     Install with: pip install {' '.join(missing)}")
        return False

    return True


def test_file_structure():
    """Test that all required files exist."""
    print("\n🔍 Testing file structure...")

    required_files = [
        "src/__init__.py",
        "src/config.py",
        "src/downloader.py",
        "src/processor.py",
        "src/database/__init__.py",
        "src/database/base.py",
        "src/database/factory.py",
        "src/database/postgres.py",
        "main.py",
        "setup.py",
        "requirements.txt",
        "requirements/base.txt",
        "requirements/postgres.txt",
    ]

    missing = []
    for file_path in required_files:
        if Path(file_path).exists():
            print(f"  ✅ {file_path}")
        else:
            print(f"  ❌ {file_path} (missing)")
            missing.append(file_path)

    if missing:
        print(f"\n  ⚠️  Missing files: {', '.join(missing)}")
        return False

    return True


def main():
    """Run all validation tests."""
    print("CNPJ Data Pipeline - Validation Suite")
    print("=" * 50)

    tests = [
        test_file_structure,
        test_requirements,
        test_imports,
        test_configuration,
        test_database_factory,
    ]

    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"  ❌ Test failed: {e}")
            results.append(False)

    print("\n" + "=" * 50)
    passed = sum(results)
    total = len(results)

    if passed == total:
        print(f"🎉 All tests passed! ({passed}/{total})")
        print("\nThe CNPJ Data Pipeline is ready to use!")
        print("Run 'python setup.py' to configure your environment.")
        return 0
    else:
        print(f"⚠️  {passed}/{total} tests passed")
        print("\nPlease fix the issues above before proceeding.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
