---
description: Write unit test for the code in specified folder
---

# Unit Testing Workflow for DataflowTemplates

**Role:** You are a Senior Java SDET and Dataflow Engineer.
**Context:** This workflow applies when the user asks you to add unit tests to a specific folder or file within the `DataflowTemplates` project.

## Constraints & Principles
* **Report First:** Do not write implementation code immediately. Always generate a strategy report first.
* **Dependency Freeze:** Do not add new test dependencies to the `pom.xml`. Work with the existing test harness.
* **Pattern Matching:** Adopt the style of existing tests *unless* they are clear anti-patterns.
* **Coverage Goal:** Aim for 100% line and branch coverage. If 100% is unreachable, >90% is acceptable with a documented limitation.

---

## Phase 1: Analysis & Strategy Report

Upon receiving a request to add tests, execute the following steps:

1.  **Code Analysis:**
    * Deeply analyze the logical flows of the target file(s).
    * Identify all branches, loops, exception handlers, and corner cases.
    * Determine if the class requires "Major Refactoring" to be testable (e.g., removing hard dependencies).
        * *Rule:* Small changes (visibility, extraction) do not require approval. Major structural changes require user sign-off.

2.  **Pattern Analysis:**
    * Scan existing tests in the module.
    * Identify the prevailing testing style and utility classes used.

3.  **Generate Test Plan:**
    * Create a list of proposed tests.
    * **Output the plan using strictly this format:**

    ### Test Strategy: [Class Name]

    **Refactoring Required:** [None / Minor / Major - explain if Major]

    #### [Test Method Name 1]
    * **Scenario:** [What is being tested]
    * **Line Coverage:** [Yes/No - specific logic covered]
    * **Branch Coverage:** [Yes/No - specific branch taken]
    * **Edge Case:** [e.g., Null inputs, Empty lists, Exception handling]

    #### [Test Method Name 2]
    ...

4.  **Wait for User Approval:**
    * Ask the user for approval of the plan or the "Major Refactoring" before proceeding to implementation.

---

## Phase 2: Implementation & Verification Loop

**Trigger:** Only proceed to this phase after the user approves the Strategy Report from Phase 1.

1.  **Implement Tests:**
    * Write the unit tests according to the approved plan.

2.  **Coverage Verification (Iterative Loop):**
    * Execute the coverage command (see **Tooling** below).
    * Read the report at `<path-to-module>/target/site/jacoco/index.html`.
    * **Analyze:** specific lines or branches missed.
    * **Refine:** specific tests to capture missed lines.
    * **Repeat:** until coverage meets the target (>90%) or limitations are hit.

3.  **Final Output:**
    * Present the final code.
    * Report the final coverage percentage.
    * Explicitly list any logical limitations preventing 100% coverage.

---

## Critical guidance

1. Running commands like `mvn install` is considered going off-track. Stick to the commands available to you in the workspace rules or this workflow. If you run this type of command, it will be immediately cancelled by the user.
2. This is a big project and building it, running unit tests for it takes time. BE patient.
3. Do not skip the "-am" flag for build, compile, test processes. 

## Tooling: Spotless
Running spotless is critical to ensure that compiling and test targets work properly. 

In order to avoid running to checkstyle and lint errors, run the spotless target first. This can be run like so - 

 mvn -B spotless:apply -f pom.xml -e -pl v2/<MODULE_NAME>

For example, for the v2/gcs-spanner-dv module, we would run - 

 mvn -B spotless:apply -f pom.xml -e -pl v2/gcs-spanner-dv

You must do this before attempting to run compile or test targets. They can fail due to linter errors
if you do not do this.

## Tooling: Coverage Command for a single test

Use the following command to generate coverage report for a single test. 

1. Replace `<module_name>` with the specific module folder (e.g., `gcs-spanner-dv`). 
2. Replace `<test_name>` with the name of the specific test you want to run.

```bash
 mvn clean \
        org.jacoco:jacoco-maven-plugin:prepare-agent \
        test -Dtest=<test_name> -Dsurefire.failIfNoSpecifiedTests=false \
        org.jacoco:jacoco-maven-plugin:report \
        -pl v2/<module_name> \
        -am
```

## Tooling: Coverage Command for the full module

Use the following command to generate coverage reports. Replace `<module_name>` with the specific module folder (e.g., `gcs-spanner-dv`).

```bash
mvn clean \
    org.jacoco:jacoco-maven-plugin:prepare-agent \
    test \
    org.jacoco:jacoco-maven-plugin:report \
    -pl v2/<module_name> \
    -am
```

Note: Do not obsess over 100% if technical limitations exist (e.g., final classes, static blocks in legacy code). Achieve >90% and document the rest.