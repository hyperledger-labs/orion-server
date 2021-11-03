import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Familiar Programming Model',
    Svg: require('../../static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        Well-known database APIs such as <code>session.Tx()</code>, <code>tx.Get()</code>,
        <code>tx.Put()</code>, <code>tx.Commit()</code> or <code>tx.Abort()</code> to build applications.
      </>
    ),
  },
  {
    title: 'Non-Repudiation',
    Svg: require('../../static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        A user who submitted a transaction to make changes to data cannot deny submitting the transaction later.
      </>
    ),
  },
  {
    title: 'Fine-Grained Read and Write Access Control',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Every data item can have a list of users who can only read the data item and a list of users who can read and write to the data item. Users who are not in these listss cannot access the data item.
      </>
    ),
  },
  {
    title: 'Multi-Signatures Transaction',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Each data item can have an access control list to dictate which users can read and write. Each user needs to authenticate themselves by providing their digital signature to read or write a given data item. Depending on the access rule defined for data item, sometimes more than one users need to authenticate themselves together to read or write to data.
      </>
    ),
  },
  {
    title: 'Rich Data Provenance',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        All historical changes to the data are maintained separately in a persisted graph data structure so that a user can execute query on those historical changes to understand the lineage of each data item.
      </>
    ),
  },
  {
    title: 'Rich JSON Queries',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        JSON queries can be used to fetch JSON data items based on certain predicates on JSON fields. Logical operators (<code>&lt;</code>,<code>&gt;</code>,<code>=</code>,<code>!=</code>,<code>&lt;=</code>,<code>&gt;=</code>) and combinational operators (<code>&&</code>, <code>||</code>) are supported on <code>string</code>, <code>boolean</code>, and <code>int64</code> data fields in a JSON value.
      </>
    ),
  },
  {
    title: 'Tamper Evident',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Data cannot be tampered with, without it going unnoticed. At any point in time, a user can request the database to provide proof for an existance of a transaction or data, and verify the same to ensure data integrity.
      </>
    ),
  },
  {
    title: 'Proofs and Verifications',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Generation and verification of proofs based on Merkle Tree and Merkle Patricia Tree for an existance or non-existance of a state, and transaction. Skip chain based proof and verification for an existance and non-existance of a block.
      </>
    ),
  },
  {
    title: 'Highly Available',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        Deployment of a cluster of Orion nodes using RAFT replication protocol to provide highly available centralized ledger service.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
