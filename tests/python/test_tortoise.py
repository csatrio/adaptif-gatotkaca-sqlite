import os as _os, sys as _sys
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
import _main  # noqa: E402,F401  (loads the pyo3 shim and the ported Python layer; skips with exit 77 when the shim is not built)

import asyncio
import os
import sys

_main.require_module("tortoise")
from tortoise import Tortoise, fields
from tortoise.exceptions import IntegrityError
from tortoise.models import Model
from tortoise.transactions import in_transaction

from sqlite.tortoise_sqlite import create_tortoise_config


class Item(Model):
    id = fields.IntField(pk=True)
    title = fields.CharField(max_length=64, unique=True)
    price = fields.FloatField(default=0.0)

    class Meta:
        table = "test_items"


async def test_tortoise_orm_with_gatotkaca():
    print("=== Testing Tortoise ORM Powered by Gatotkaca SQLite ===")

    # 1. Initialize with in-memory database
    config = create_tortoise_config(
        db_path="file:tortoise_test_mem?mode=memory&cache=shared",
        model_modules=["__main__"],
        max_size=5,
        wal=False,
    )
    await Tortoise.init(config=config)
    await Tortoise.generate_schemas()
    print("✓ Initialized Tortoise and generated schemas")

    # 2. Create records
    item1 = await Item.create(title="Gatotkaca Server", price=99.99)
    item2 = await Item.create(title="Second Item", price=49.99)
    assert item1.id is not None
    assert item2.id is not None
    print(f"✓ Created items: {item1.id}, {item2.id}")

    # 3. Query records
    items = await Item.all().order_by("id")
    assert len(items) == 2
    assert items[0].title == "Gatotkaca Server"
    assert items[1].title == "Second Item"
    print(f"✓ Queried all items successfully: {[i.title for i in items]}")

    # 4. Filter
    filtered = await Item.filter(price__gt=50.0)
    assert len(filtered) == 1
    assert filtered[0].title == "Gatotkaca Server"
    print("✓ Filtered items successfully")

    # 5. Transactions (Commit & Rollback)
    async with in_transaction():
        await Item.create(title="Committed Item", price=10.0)

    committed = await Item.filter(title="Committed Item").first()
    assert committed is not None
    print("✓ Transaction committed successfully")

    try:
        async with in_transaction():
            await Item.create(title="Rolled Back Item", price=20.0)
            raise RuntimeError("Intentional rollback test")
    except RuntimeError:
        pass

    rolled_back = await Item.filter(title="Rolled Back Item").first()
    assert rolled_back is None
    print("✓ Transaction rollback verified successfully")

    # 6. Integrity errors, bulk insert (execute_many), nested transactions (savepoints), raw SQL
    try:
        await Item.create(title="Gatotkaca Server")
        raise AssertionError("duplicate title must fail")
    except IntegrityError:
        pass
    await Item.bulk_create([Item(title=f"bulk-{i}", price=float(i)) for i in range(50)])
    assert await Item.filter(title__startswith="bulk-").count() == 50
    async with in_transaction():
        await Item.create(title="outer")
        try:
            async with in_transaction():
                await Item.create(title="inner")
                raise RuntimeError("undo inner only")
        except RuntimeError:
            pass
    assert await Item.filter(title="outer").exists() and not await Item.filter(title="inner").exists()
    conn = Tortoise.get_connection("default")
    assert (await conn.execute_query_dict("SELECT count(*) AS n FROM test_items WHERE title = ?", ["outer"]))[0]["n"] == 1
    print("✓ Integrity errors, bulk insert, savepoints and raw SQL verified")

    # 7. Close connections
    await Tortoise.close_connections()
    print("=== Tortoise ORM Tests PASSED 100% Successfully! ===")


if __name__ == "__main__":
    asyncio.run(test_tortoise_orm_with_gatotkaca())
