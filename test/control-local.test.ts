// Control: runs the same conformance suite against the reference world so a
// failure can be attributed to the adapter rather than the harness.
import { createTestSuite } from '@workflow/world-testing';

createTestSuite('@workflow/world-local');
