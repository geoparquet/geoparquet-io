# Phase 2: Decision Points & Questions

**Purpose**: Clarify choices before starting Phase 3 (Implementation)
**Audience**: Nissim + Chris (product decisions)
**Date**: 2026-01-18

---

## Timeline & Scope Decisions

### Q1. What's your timeline preference for v1.0-beta.1?

Three options based on effort analysis:

**Option A: Quick Release** (~1 week, 20-27 hours)
- Fix only P0 critical items
- Release beta with known issues
- Patch in follow-up releases

**Option B: Hybrid Approach** (~1.5 weeks, 35-40 hours) ⭐ **RECOMMENDED**
- Fix all P0 critical items
- Add high-impact quick wins (error handling, inspect bug)
- Defer complex refactoring for patches

**Option C: Full Quality Pass** (~3 weeks, 50-67 hours)
- Fix all P0 and P1 items
- No known critical issues at release
- Strong foundation for 1.x series

**Decision needed**: Which option aligns with your release goals?

---

### Q2. What's acceptable test coverage for v1.0-beta.1?

**Current state**: 72.78% (2.22 points below 75% threshold)

**Options**:

A. **Strict enforcement**: Must hit 75%+ before release
   - Effort: 8-10 hours to add ~50 tests
   - Ensures quality bar is met
   - Delays release by ~1-2 days

B. **Pragmatic approach**: Accept 72-74% for beta, reach 75% by 1.0 final
   - Faster to beta
   - Risk: Technical debt if not addressed

C. **Strategic coverage**: Focus only on critical gaps (api/check.py, core/common.py)
   - Effort: 4-5 hours for critical modules
   - Brings coverage to ~74%
   - Good middle ground

**Decision needed**: What's your coverage requirement for beta vs final release?

---

## API Design & Breaking Changes

### Q3. Deprecation strategy for parameter names?

The checklist recommends fixing CLI inconsistencies (Issue #120). This involves renaming parameters.

**Options**:

A. **Hard break** (rename immediately, no backward compatibility)
   - Clean API for v1.0
   - Users must update scripts
   - ❌ Breaks existing workflows

B. **Deprecation warnings** (support both old and new for 1-2 releases)
   - Keep old names with warnings
   - Remove in v1.1 or v2.0
   - ✅ Smooth migration path
   - ⚠️ More code to maintain

C. **Defer to v2.0** (keep current names, mark as "known issue")
   - No changes for v1.x series
   - Fix in next major version
   - Avoids churn

**Decision needed**:
- Can we introduce breaking changes in v1.0-beta.1?
- Should we use deprecation warnings, or hard breaks?
- What's the migration timeline (immediate, v1.1, v2.0)?

---

### Q4. Should we remove deprecated commands now or later?

**Context**: 5 deprecated commands exist (Issue #154):
- `gpio reproject` → `gpio convert reproject`
- `gpio meta` → `gpio inspect meta`
- `gpio stac` → `gpio publish stac`
- `gpio upload` → `gpio publish upload`
- `gpio validate` → `gpio check spec`

**Options**:

A. **Remove in v1.0-beta.1** (recommended for beta)
   - Clean slate for 1.0 series
   - Users must migrate to new commands
   - Beta is the time for breaking changes
   - Effort: 2-3 hours

B. **Keep with warnings through v1.0, remove in v1.1**
   - Gentler migration
   - More code to maintain
   - Less clean for "1.0" branding

C. **Keep indefinitely** (not recommended)
   - No breaking changes
   - Confusing dual command structure
   - Technical debt

**Decision needed**: Remove deprecated commands now, or keep them for v1.0?

---

### Q5. How should we handle the `--profile` flag cleanup?

**Context**: `--profile` appears on many commands but is only needed for S3 operations (Issue #150).

**Options**:

A. **Remove from non-S3 commands** (breaking change)
   - Cleaner API
   - May break user scripts that pass `--profile` everywhere
   - Effort: 3-4 hours

B. **Keep but ignore on non-S3 commands** (non-breaking)
   - Backward compatible
   - Confusing UX (flag exists but does nothing)
   - Doesn't solve the problem

C. **Rename to `--aws-profile` and keep only on S3 commands**
   - Clearest API
   - Breaking change
   - Requires deprecation period

D. **Defer to v2.0**
   - No changes for v1.x
   - Keeps technical debt

**Decision needed**:
- Remove, rename, or keep `--profile`?
- Is this a breaking change we can make in beta?

---

## Technical Architecture

### Q6. Should we split `core/common.py` before v1.0?

**Context**: 3,441 lines with 77 functions. Proposed split into specialized modules.

**Options**:

A. **Split now** (before v1.0-beta.1)
   - Cleaner architecture for 1.0
   - Risk: Breaking internal imports
   - Effort: 8-10 hours
   - Delays beta release

B. **Split after v1.0-beta.1** (in v1.0.1 or v1.1)
   - Faster to beta
   - Maintain backward compatibility via imports
   - Refactor when less time pressure

C. **Don't split** (keep as-is)
   - No risk of breaking changes
   - File is large but functional
   - Technical debt remains

**Decision needed**: Is file organization a blocker for v1.0, or can we defer?

---

### Q7. How to handle the Grade E complexity function?

**Context**: `cli/main.py:2033` - `inspect_legacy()` has critical complexity (Grade E).

**Options**:

A. **Refactor before beta** (recommended)
   - Meets quality standards
   - Reduces maintenance burden
   - Effort: 4-6 hours
   - May introduce bugs if rushed

B. **Ship with Grade E, refactor in patch**
   - Faster to beta
   - Known technical debt
   - Pre-commit hook currently allows it (max E)

C. **Deprecate the function** (if it's truly legacy)
   - Remove instead of refactor
   - Effort: 2 hours
   - Loses functionality

**Decision needed**:
- Is this function still needed, or can we remove it?
- If keeping, must we refactor before beta?

---

## Feature Scope

### Q8. Should we investigate/fix the large dataset issue (Issue #169)?

**Context**: 45GB GPKG conversion fails with 15GB RAM due to Arrow buffer limits.

**Options**:

A. **Block beta until fixed** (high effort)
   - Ensures large file support
   - Effort: 6-8 hours investigation + unknown fix time
   - May require architectural changes
   - Risk: Could delay beta significantly

B. **Document limitations clearly** (quick fix)
   - Add memory limit warnings
   - Document workarounds
   - Effort: 2-3 hours
   - Users know what to expect

C. **Investigate but don't block beta**
   - Research in parallel
   - Fix in v1.0.1 if solution found
   - Release beta with known limitation

**Decision needed**:
- Is large file support required for v1.0-beta.1?
- Or can we ship with documented limitations?

---

### Q9. Should we add GeoPandas interoperability (Issue #141)?

**Context**: Users request `table.to_geopandas()` and `Table.from_geopandas()`.

**Options**:

A. **Add before v1.0-beta.1**
   - Complete Python API
   - Effort: 6-8 hours
   - Delays beta

B. **Add in v1.1** (recommended)
   - Not critical for 1.0
   - Get user feedback first
   - Lower risk

C. **Won't implement**
   - Users can use Arrow/Parquet directly
   - Avoids soft dependency

**Decision needed**: Is GeoPandas integration required for v1.0?

---

## Quality Standards

### Q10. What's the acceptable complexity threshold?

**Context**: Project currently allows up to Grade E in pre-commit. Analysis found:
- 1 Grade E function
- 9 Grade D functions
- 86 Grade C functions

**Options**:

A. **Enforce Grade A** (strictest)
   - All functions simple and maintainable
   - Effort: 40-50 hours refactoring
   - Not realistic for beta timeline

B. **Enforce Grade B** (strict but achievable)
   - No Grade D or E functions allowed
   - Effort: 16-20 hours to fix Grade D
   - Recommended long-term standard

C. **Enforce Grade C** (moderate)
   - No Grade D or E functions
   - Accept Grade C for complex domain logic
   - Effort: 4-6 hours to fix Grade E
   - Realistic for beta

D. **Keep current (Grade E allowed)**
   - No changes needed
   - Technical debt remains
   - Not recommended for 1.0

**Decision needed**: What complexity grade is acceptable for v1.0-beta.1?

---

### Q11. Error handling philosophy?

**Context**: Issue #140 reports stack traces for common user errors.

**Options**:

A. **Strict validation** (catch everything, never show stack traces)
   - Professional UX
   - Effort: 3-4 hours
   - May hide legitimate errors

B. **Helpful errors with stack trace option**
   - User-friendly messages by default
   - `--debug` flag shows full stack trace
   - Good middle ground

C. **Current behavior** (show stack traces)
   - Debugging-friendly
   - Poor user experience
   - Not recommended

**Decision needed**: How strict should input validation be?

---

## Documentation Standards

### Q12. Is 86% documentation coverage acceptable?

**Current state**: 12 of 14 guide files have both CLI and Python examples.

**Missing**:
- CLI counterparts in Python API docs for partition methods
- Python alternatives noted in CLI-only guides

**Options**:

A. **Require 100% before beta**
   - Professional documentation
   - Effort: 2-3 hours
   - Minimal delay

B. **Ship with 86%, complete in patches**
   - Faster to beta
   - Known documentation gaps

C. **Strategic completion** (fix critical gaps only)
   - Add partition CLI examples
   - Effort: 1-2 hours
   - Good middle ground

**Decision needed**: Documentation completeness requirement for beta?

---

### Q13. Should all methods have inline docstring examples?

**Context**: Most methods have docstrings, but some complex ones could use more examples.

**Options**:

A. **Add examples to all methods** (comprehensive)
   - Effort: 4-6 hours
   - Self-documenting code
   - Delays beta

B. **Add examples to complex methods only** (strategic)
   - Focus on BigQuery, ArcGIS, partitioning
   - Effort: 2-3 hours
   - Good ROI

C. **Keep current state**
   - Rely on guide docs
   - Faster to beta

**Decision needed**: What's the docstring example coverage requirement?

---

## Testing Strategy

### Q14. Should we test the Python API comprehensively?

**Context**: API has 30/40 Table methods tested (75%), missing partition and BigQuery tests.

**Options**:

A. **Test all API methods** (comprehensive)
   - 100% API coverage
   - Effort: 6-8 hours
   - Ensures API stability

B. **Test critical methods only** (strategic)
   - Focus on commonly-used features
   - Effort: 3-4 hours
   - Acceptable gaps in advanced features

C. **Current coverage is sufficient**
   - 75% is passing grade
   - Faster to beta

**Decision needed**: What % of Python API must be tested?

---

### Q15. How should we handle tests for external services?

**Context**: Some features require BigQuery, ArcGIS, or S3 access.

**Options**:

A. **Mock all external services**
   - 100% testable locally
   - Effort: High (mocking is complex)
   - May miss integration issues

B. **Skip external service tests with `@pytest.mark.skip`**
   - Current approach
   - Document requirements clearly
   - Run in CI with credentials

C. **Require manual testing** (no automated tests)
   - Not recommended
   - Regression risk

**Decision needed**: Keep current skip approach, or invest in mocking?

---

## Release Process

### Q16. What defines "beta" vs "final" for v1.0?

**Clarify expectations**:

**v1.0-beta.1 should have**:
- [ ] All deprecated commands removed?
- [ ] Test coverage ≥75%?
- [ ] No Grade E complexity?
- [ ] All CLI parameters consistent?
- [ ] No known critical bugs?
- [ ] 100% documentation?

**OR is beta for**:
- Gathering user feedback
- Testing in production
- Finding unknown issues
- Accepting known limitations

**Decision needed**: What's the quality bar for beta vs final release?

---

### Q17. How long is the beta period?

**Options**:

A. **Short beta** (1-2 weeks)
   - Quick feedback cycle
   - Fast iteration to v1.0 final
   - Risk: May miss issues

B. **Medium beta** (1 month)
   - Reasonable feedback window
   - Balance speed and quality

C. **Long beta** (2-3 months)
   - Thorough validation
   - Slower to v1.0 final

**Decision needed**: Timeline for beta → v1.0-rc.1 → v1.0 final?

---

## Implementation Priorities

### Q18. Should I tackle tasks sequentially or in parallel?

**Context**: 15 tasks identified, varying effort.

**Options**:

A. **Sequential by priority** (P0 → P1 → P2 → P3)
   - Clear focus
   - Finish critical items first
   - May be slower overall

B. **Parallel tracks** (complex + quick wins simultaneously)
   - Faster overall completion
   - Risk: Context switching
   - Requires careful coordination

C. **Quick wins first** (build momentum)
   - Early visible progress
   - Defer complex refactoring
   - Good morale

**Decision needed**: What's your preferred implementation order?

---

### Q19. Should we create GitHub issues for tracking?

**Options**:

A. **Create issues for all tasks**
   - Public tracking
   - Can assign and track progress
   - Visible to community
   - Effort: 1-2 hours setup

B. **Use this checklist only**
   - Simpler
   - Private planning
   - Less overhead

C. **Issues for P0/P1 only**
   - Track critical items
   - Skip low-priority tasks
   - Good middle ground

**Decision needed**: How do you want to track implementation progress?

---

### Q20. Do you want me to implement, or should I create branches/PRs?

**Workflow options**:

A. **I implement directly on your behalf**
   - Fastest progress
   - You review after completion
   - Risk: May not match your style

B. **I create feature branches, you review before merge**
   - More control
   - Longer iteration cycle
   - Better for learning

C. **Pair programming style** (implement together)
   - Real-time feedback
   - Slower but educational
   - Best alignment with your goals

**Decision needed**: What's your preferred collaboration model for Phase 3?

---

## Summary of Required Decisions

### Critical (Must decide before starting):

1. ✅ **Timeline**: Option A/B/C for release schedule?
2. ✅ **Test coverage**: 72%, 74%, or 75% requirement?
3. ✅ **Breaking changes**: Allowed in beta?
4. ✅ **Deprecated commands**: Remove now or later?
5. ✅ **Complexity threshold**: Grade B or C for beta?

### Important (Affects scope):

6. ⚠️ **Large dataset issue**: Block beta or document?
7. ⚠️ **GeoPandas**: Add now or defer?
8. ⚠️ **Documentation**: 100% or 86%?
9. ⚠️ **Error handling**: Strict or permissive?

### Process (Affects workflow):

10. 🔧 **Implementation order**: Sequential, parallel, or quick wins first?
11. 🔧 **Tracking**: GitHub issues or checklist only?
12. 🔧 **Collaboration**: Direct implementation, branches, or pair programming?

---

## Recommended Decision Template

Copy this and fill it out:

```markdown
## Phase 3 Implementation Decisions

**Timeline**: [Option A/B/C]
**Test coverage target**: [72%/74%/75%]
**Breaking changes in beta**: [Yes/No]
**Remove deprecated commands**: [Now/v1.1/Never]
**Complexity threshold**: [Grade B/C]

**Large dataset issue**: [Block/Document/Investigate in parallel]
**GeoPandas support**: [v1.0-beta.1/v1.1/Won't add]
**Documentation requirement**: [100%/86%/Strategic gaps]
**Error handling**: [Strict validation/Helpful with --debug/Current]

**Implementation approach**: [Sequential/Parallel/Quick wins first]
**Progress tracking**: [GitHub issues all/issues P0-P1/Checklist only]
**Collaboration model**: [Direct implementation/Feature branches/Pair programming]

**Additional notes**:
-
-
```

---

**Next Step**: Fill out this template, then I'll start Phase 3 implementation with clear guardrails.
