# Python Coding Rules

## Design

- Follow the guidelines in "A Philosophy of Software Design."
- Consider coupling and cohesion, and watch for code smells.

## Type Hints

- Add type hints for all function arguments and return values.
- Avoid using `Any` in principle.
- Do not use primitive types directly; define meaningful types.
- Avoid collections like `dict[str, str]` where the contents are unclear.
    - Prefer defining and using dataclass or TypedDict models.
- Do not use primitive types for business-meaningful elements.
    - Prefer defining dedicated types.
    - Example: `UserId = NewType("UserId", int)`
- For nullable types, prefer `int | None` over `Optional[int]`.
- For nullable types, do not distinguish between `None` and empty unless it is meaningful for the business domain.
    - To represent "no value," prefer `str` with an empty string over `str | None`.
    - Likewise, prefer `list` with an empty list over `list | None` (same for other collections).
    - Use nullable types when there is no real "empty" value, such as for `int`.
- Prefer Protocol over ABC.
- Prioritize visibility. In modules and classes, make methods and variables private unless they must be public.

## Code Quality

### Naming Conventions

- For file- and class-level private functions and variables, use a leading underscore.
    - Treat everything as private by default and make it public only when needed.

### Logging

Use `logger` for output.

```python
import logging
_logger = logging.getLogger(__name__)
_logger.info("info message")
```

### Error Handling

- If exceptions cannot be handled appropriately at that layer from a business perspective, let them propagate without catching.
    - Catching only to log a single line and then re-raising is prohibited.
