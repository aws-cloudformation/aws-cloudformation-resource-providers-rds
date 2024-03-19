## aws-cloudformation-resource-providers-rds

The CloudFormation Resource Provider Package For Amazon Relational Database Service

## License

This library is licensed under the Apache 2.0 License.

### Generate testsAccountsConfig.yml for contract tests

See [Uluru wiki](https://w.amazon.com/bin/view/AWS/CloudFormation/Teams/ProviderEx/RP-Framework/Projects/UluruContractTests#HCanIrunCTv2inpipelineusingmyownaccounts3F)

Uluru allows service teams to run contract tests on their own accounts. This way, the test process is completely visible
to the service team -- any errors can be easily debugged in Step Functions (instead of S3), any stuck dependency stacks
can be freely removed and retried, and contract tests can reuse the same prefab resources as integration tests.

File generation is only needed if: 1) RDS adds a new control plane region, 2) RDS adds a new CFN resource

1. (One-time) Install jq and yq
   ```
   brew install jq yq
   ```
2. Run command to generate testsAccountsConfig.yml and copy the generated file to all projects' **contract-tests-artifacts** directories
   ```
   brazil-build generateTestAccountsConfig
   ```
3. Examine `git diff` to make sure the changes are expected
4. CR the changes

## IntelliJ Setup

As long as you are using the latest BlackCaiman, it should "just work".
1. Run `brazil-build` once at the root level.
1. Open it just like any other Java package.
1. Use the menu Brazil -> Sync from workspace (Enhanced)

It's using the [Enhanced Sync from Workspace](https://builderhub.corp.amazon.com/docs/black-caiman/user-guide/enhanced-sync-from-workspace.html#override) so you must use
a BlackCaiman version 2023.3.186.0.2023.2 or more recent.

If you are adding a new resource type, you must edit the file `.bemol.toml` to add the project source and test files.
You can simply copy an existing example.

### Running unit tests

It should just work. The unit nest needs "aws-rds-RESOURCE/target/schema" specifically in the classpath (handled by the bemol config file).

### My local state is really strange!

First make sure all your changes are saved/committed. There is no going back.

There are many files that are in .gitignore. To truly clean your workspace, run the following:
```
brazil ws clean
git clean -dfx
```
