/*
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

module.exports = {
    Documentation: [
        "introduction",
        {
            type: "category",
            label: "Getting Started",
            items: [
                "getting-started/guide",
                {
                    type: "category",
                    label: "Launching a Orion Node",
                    items: [
                        "getting-started/launching-one-node/overview",
                        "getting-started/launching-one-node/binary",
                        "getting-started/launching-one-node/docker",
                        "getting-started/launching-one-node/crypto-materials",
                    ],
                },
                {
                    type: "category",
                    label: "Launching a Orion Cluster",
                    items: [
                        "getting-started/launching-cluster/overview",
                        "getting-started/launching-cluster/private-configuration",
                        "getting-started/launching-cluster/shared-configuration",
                        "getting-started/launching-cluster/crypto-materials",
                        "getting-started/launching-cluster/docker-compose",
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
                                "getting-started/transactions/curl/dbtx",
                                "getting-started/transactions/curl/usertx",
                                "getting-started/transactions/curl/datatx",
                                "getting-started/transactions/curl/configtx",
                            ],
                        },
                        {
                            type: "category",
                            label: "Using Go SDK",
                            items: [
                                "getting-started/transactions/gosdk/dbtx",
                                "getting-started/transactions/gosdk/usertx",
                                "getting-started/transactions/gosdk/datatx",
                                "getting-started/transactions/gosdk/configtx",
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
                                "getting-started/queries/curl/cluster-config",
                                "getting-started/queries/curl/node-config",
                                "getting-started/queries/curl/user",
                                "getting-started/queries/curl/db",
                                "getting-started/queries/curl/simple-data-query",
                                "getting-started/queries/curl/complex-data-query",
                                "getting-started/queries/curl/provenance",
                                "getting-started/queries/curl/block-header",
                                "getting-started/queries/curl/transaction-receipt",
                            ],
                        },
                        {
                            type: "category",
                            label: "Using Go SDK",
                            items: [
                                "getting-started/queries/gosdk/cluster-config",
                                "getting-started/queries/gosdk/node-config",
                                "getting-started/queries/gosdk/user",
                                "getting-started/queries/gosdk/db",
                                "getting-started/queries/gosdk/simple-data-query",
                                "getting-started/queries/gosdk/complex-data-query",
                                "getting-started/queries/curl/provenance",
                                "getting-started/queries/gosdk/block-header",
                                "getting-started/queries/gosdk/transaction-receipt",
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
                                "getting-started/proofs-and-verification/curl/state",
                                "getting-started/proofs-and-verification/curl/tx",
                                "getting-started/proofs-and-verification/curl/block",
                            ],
                        },
                        {
                            type: "category",
                            label: "Using Go SDK",
                            items: [
                                "getting-started/proofs-and-verification/gosdk/state",
                                "getting-started/proofs-and-verification/gosdk/tx",
                                "getting-started/proofs-and-verification/gosdk/block",
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
                "design-principles/programming-model",
                "design-principles/simpler-deployment",
                "design-principles/json-data-model",
            ],
        },
        {
            type: "category",
            label: "User Cases",
            items: [
                "use-cases/document-management",
                "use-cases/government-agencies",
                "use-cases/invoicing",
                "use-cases/asset-transfer",
            ],
        },
        {
            type: "category",
            label: "Architecture and Design",
            items: [
                "architecture-and-design/internals",
                "architecture-and-design/transaction-flow",
                "architecture-and-design/query-flow",
                "architecture-and-design/block-structure",
                "architecture-and-design/transactions-structure",
                "architecture-and-design/query-syntax",
                "architecture-and-design/acl-read",
                "architecture-and-design/acl-write",
                "architecture-and-design/multi-sign-datatx",
                "architecture-and-design/state-merkle-patricia-tree",
                "architecture-and-design/tx-merkle-tree",
                "architecture-and-design/block-skip-chain",

            ],
        },
        {
            type: "category",
            label: "Security and Trust Model",
            items: [
                "security-and-trust-model/authentication",
                "security-and-trust-model/access-control",
                "security-and-trust-model/non-repudiation",
                "security-and-trust-model/tamper-evident",
                "security-and-trust-model/double-spend-prevention",
                "security-and-trust-model/data-provenance",
                "security-and-trust-model/proofs-and-verifications",
            ],
        },
        "rest-api-specifications",
        "roadmap",
    ],
}
