import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Familiar Programming Model',
    Svg: require('../../static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Non-Repudiation',
    Svg: require('../../static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Fine-Grained Read and Write Access Control',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Multi-Signatures Transaction',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Rich Data Provenance',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Rich JSON Queries',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Tamper Evident',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Proofs and Verifications',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
      </>
    ),
  },
  {
    title: 'Highly Available',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        TODO
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
