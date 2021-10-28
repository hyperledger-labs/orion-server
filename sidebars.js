/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

module.exports = {
    Documentation: [
        "external/introduction",
        {
            type: "category",
            label: "Getting Started",
            items: [
                "external/getting-started/guide",
                {
                    type: "category",
                    label: "Launching a Orion Node",
                    items: [
                        "external/getting-started/launching-one-node/overview",
                        "external/getting-started/launching-one-node/binary",
                        "external/getting-started/launching-one-node/docker",
                        "external/getting-started/launching-one-node/crypto-materials",
                    ],
                },
                {
                    type: "category",
                    label: "Launching a Orion Cluster",
                    items: [
                        "external/getting-started/launching-cluster/overview",
                        "external/getting-started/launching-cluster/private-configuration",
                        "external/getting-started/launching-cluster/shared-configuration",
                        "external/getting-started/launching-cluster/crypto-materials",
                        "external/getting-started/launching-cluster/docker-compose",
                    ],
                },
                {
                    type: "category",
                    label: "Executing Transactions",
                    items: [
                        {
                            type: "category",
                            label: "Using Curl command",
                            items: [
                                "external/getting-started/transactions/curl/dbtx",
                                "external/getting-started/transactions/curl/usertx",
                                "external/getting-started/transactions/curl/datatx",
                                "external/getting-started/transactions/curl/configtx",
                            ],
                        },
                        {
                            type: "category",
                            label: "Using Go SDK",
                            items: [
                                "external/getting-started/transactions/gosdk/dbtx",
                                "external/getting-started/transactions/gosdk/usertx",
                                "external/getting-started/transactions/gosdk/datatx",
                                "external/getting-started/transactions/gosdk/configtx",
                            ],
                        },
                    ],
                },
                {
                    type: "category",
                    label: "Executing Queries",
                    items: [
                        {
                            type: "category",
                            label: "Using Curl command",
                            items: [
                                "external/getting-started/queries/curl/cluster-config",
                                "external/getting-started/queries/curl/node-config",
                                "external/getting-started/queries/curl/user",
                                "external/getting-started/queries/curl/db",
                                "external/getting-started/queries/curl/simple-data-query",
                                "external/getting-started/queries/curl/complex-data-query",
                                "external/getting-started/queries/curl/block-header",
                                "external/getting-started/queries/curl/transaction-receipt",
                            ],
                        },
                        {
                            type: "category",
                            label: "Using Go SDK",
                            items: [
                                "external/getting-started/queries/gosdk/cluster-config",
                                "external/getting-started/queries/gosdk/node-config",
                                "external/getting-started/queries/gosdk/user",
                                "external/getting-started/queries/gosdk/db",
                                "external/getting-started/queries/gosdk/simple-data-query",
                                "external/getting-started/queries/gosdk/complex-data-query",
                                "external/getting-started/queries/gosdk/block-header",
                                "external/getting-started/queries/gosdk/transaction-receipt",
                            ],
                        },
                    ],
                },
                {
                    type: "category",
                    label: "Executing Proof Generation and Verification",
                    items: [
                        {
                            type: "category",
                            label: "Using Curl command",
                            items: [
                                "external/getting-started/proofs-and-verification/curl/state",
                                "external/getting-started/proofs-and-verification/curl/tx",
                                "external/getting-started/proofs-and-verification/curl/block",
                            ],
                        },
                        {
                            type: "category",
                            label: "Using Go SDK",
                            items: [
                                "external/getting-started/proofs-and-verification/gosdk/state",
                                "external/getting-started/proofs-and-verification/gosdk/tx",
                                "external/getting-started/proofs-and-verification/gosdk/block",
                            ],
                        },
                    ],
                },
            ],
        },
        {
            type: "category",
            label: "Design Principes",
            items: [
                "external/design-principles/programming-model",
                "external/design-principles/simpler-deployment",
                "external/design-principles/json-data-model",
            ],
        },
        {
            type: "category",
            label: "User Cases",
            items: [
                "external/use-cases/document-management",
                "external/use-cases/government-agencies",
                "external/use-cases/invoicing",
                "external/use-cases/asset-transfer",
            ],
        },
        {
            type: "category",
            label: "Architecture and Design",
            items: [
                "external/architecture-and-design/components",
                "external/architecture-and-design/transaction-flow",
                "external/architecture-and-design/query-flow",
                "external/architecture-and-design/block-structure",
                "external/architecture-and-design/transactions-structure",
                "external/architecture-and-design/query-syntax",
                "external/architecture-and-design/acl-read",
                "external/architecture-and-design/acl-write",
                "external/architecture-and-design/multi-sign-datatx",
                "external/architecture-and-design/state-merkle-patricia-tree",
                "external/architecture-and-design/tx-merkle-tree",
                "external/architecture-and-design/block-skip-chain",

            ],
        },
        {
            type: "category",
            label: "Security and Trust Model",
            items: [
                "external/security-and-trust-model/authentication",
                "external/security-and-trust-model/access-control",
                "external/security-and-trust-model/non-repudiation",
                "external/security-and-trust-model/tamper-evident",
                "external/security-and-trust-model/double-spend-prevention",
                "external/security-and-trust-model/data-provenance",
                "external/security-and-trust-model/proofs-and-verifications",
            ],
        },
        "external/rest-api-specifications",
        "external/roadmap",
    ],
}
