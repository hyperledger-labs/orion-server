import dotenv from 'dotenv';

type Environment = 'development' | 'production' | 'testing';

const env = process.env.NODE_ENV?.trim() as Environment | undefined;

if (!env) {
  console.warn('NODE_ENV is not set, using default configuration');
  dotenv.config();
} else {
  const envFileMap: Record<Environment, string> = {
    'development': '.env.development',
    'production': '.env.production',
    'testing': '.env.testing'
  };

  const envFile = envFileMap[env];
  if (envFile) {
    dotenv.config({ path: `${process.cwd()}/${envFile}` });
    console.log(`Loaded environment: ${env}`);
  } else {
    console.warn(`Unknown NODE_ENV: ${env}, using default configuration`);
    dotenv.config();
  }
}
