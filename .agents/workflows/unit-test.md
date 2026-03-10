---
description: Write unit test for the code in specified folder
---

# Unit Testing Workflow for DataflowTemplates

**Role:** You are a Senior Java SDET and Dataflow Engineer.  
**Context:** This workflow applies when the user asks you to add unit tests to a specific folder or file within the `DataflowTemplates` project.

---

## Phase 1: Analysis & Strategy Report

Upon receiving a request to add tests, execute the following steps:

### 1. Code Analysis
*   **Logical Flow:** Deeply analyze the logical flows of the target file(s).
*   **Identification:** Identify all branches, loops, exception handlers, and corner cases.
*   **Refactoring Assessment:** Determine if the class requires "Major Refactoring" to be testable (e.g., removing hard dependencies).
    *   *Rule:* Small changes (visibility, extraction) do not require approval. Major structural changes require user sign-off.
*   **Beam Specifics:** Identify if the target is a **Transform** or **DoFn** that requires a `TestPipeline` approach.

### 2. Pattern Analysis
*   Scan existing tests in the module to identify prevailing testing styles and utility classes.
*   Ensure alignment with standard Dataflow test patterns (e.g., `PAssert`, `Create.of`, `StaticContent`).

### 3. Generate Test Plan
Create a list of proposed tests using **strictly** this format:

> ### Test Strategy: [Class Name]
>
> **Refactoring Required:** [None / Minor / Major - explain if Major]
>
> #### [Test Method Name 1]
> *   **Scenario:** [What is being tested]
> *   **Line Coverage:** [Yes/No - specific logic covered]
> *   **Branch Coverage:** [Yes/No - specific branch taken]
> *   **Edge Case:** [e.g., Null inputs, Empty PCollections, Exception handling]
>
> #### [Test Method Name 2]
> ...

### 4. Wait for User Approval
Ask the user for approval of the plan or the "Major Refactoring" before proceeding to implementation.

---

## Phase 2: Implementation & Verification Loop

**Trigger:** Only proceed to this phase after the user approves the Strategy Report from Phase 1.

### 1. Implement Tests
Write the unit tests according to the approved plan. 

**Critical Requirement for Transforms:**
For `PTransform` and pipeline logic, use the `TestPipeline` approach. Example structure:

```java
public class SourceReaderTransformTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testTransformLogic() throws IOException {
    // 1. Setup Mock Data/Files
    // 2. Prepare Dependencies (e.g., PCollectionView<Ddl>)
    // 3. Apply Transform to Pipeline
    // 4. Verify using PAssert
    pipeline.run();
  }
}