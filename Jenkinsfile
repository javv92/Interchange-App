pipeline {
    agent any

    options {
       disableConcurrentBuilds()
    }

    environment {
      ENVIRONMENT = getEnvironment()
      APP_STACK_NAME = "app-interchange-stack-${ENVIRONMENT}"
      DEVOPS_STACK_NAME = 'app-interchange-devops-stack'
    }

    stages {

        stage('Preparing Configurations'){
          steps {
            script{
                SHORT_COMMIT = getShortCommitId()
                LAMBDA_APP_ZIP_NAME = "Lambda-app-${SHORT_COMMIT}.zip"
                APP_ZIP_NAME = "App-${SHORT_COMMIT}.zip"

                //sh (returnStdout: true, script: "aws s3 cp s3://itl-0009-devops-all-s3-main-01/app-interchange/config/${ENVIRONMENT}/.env ${workspace}/.env", label: "Download config file")                
                sh (returnStdout: true, script: "aws s3 ls", label: "Download config file")                
            }
          }
        }

        stage('Infrastructure Provisioning'){
            steps {
                script {
                    sh (returnStdout: true, script: "aws s3 cp infrastructure/cfn-main-template.yaml s3://itl-0009-devops-all-s3-main-01/app-interchange/cloudformation-templates/${ENVIRONMENT}/cfn-main-template.yaml", label: "Uploading from s3")

                    dir('infrastructure'){

                          appStackStatus = getStackStatus(APP_STACK_NAME)

                          if(appStackStatus == "ROLLBACK_COMPLETE"){
                              sh (returnStdout: false, script: "aws cloudformation delete-stack --stack-name ${APP_STACK_NAME}", label: "Delete Stack ..." )

                              sh (returnStdout: false, script: "aws cloudformation wait stack-delete-complete --stack-name ${APP_STACK_NAME}", label: "Waiting for Stack Deleted ..." )
                          }

                          def deployScriptString = "aws cloudformation deploy --template-file ./cfn-main-template.yaml --stack-name ${APP_STACK_NAME} --capabilities CAPABILITY_NAMED_IAM \
                              --parameter-overrides ${readYamlParameters()} \
                              --tags Environment=${ENVIRONMENT} Project=AppInterchange"

                          sh (returnStdout: false, script: deployScriptString, label: "Deploying App stack ..." )

                          appStackStatus = getStackStatus(APP_STACK_NAME)

                          getStackStatus = sh (returnStdout: true, script: "aws cloudformation describe-stacks --stack-name ${APP_STACK_NAME}", label: "Describe App Stack").trim()
                          jsonResult = readJSON text: getStackStatus

                          switch (ENVIRONMENT) {
                              case "dev":
                                  rds_endpoint = "app-interchange-aurora-priv-cluster-dev-cluster.cluster-cf3zxr6zcsiz.us-east-1.rds.amazonaws.com"
                                  break
                              case "prd":
                                  rds_endpoint = "app-interchange-aurora-priv-cluster-prd-cluster.cluster-cf3zxr6zcsiz.us-east-1.rds.amazonaws.com"
                                  break
                              default:
                                  rds_endpoint = ""
                          }

                          LAMBDA_APP_ARN = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'LambdaFunctionName'}.OutputValue
                          OPENSEARCH_URL = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'OpenSearchDomainEndpoint'}.OutputValue
                          OPENSEARCH_SECRET = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'OpenSearchSecret'}.OutputValue
                          RDS_AURORA_SECRET = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'RdsAuroraSecret'}.OutputValue
                          RDS_INSTANCE_ENDPOINT = rds_endpoint
                    }
                }
            }
        }

        stage('Deploying Lambda App') {
            steps {
                script {
                    dir('Lambdas/lambda_app'){
                        sh (returnStdout: false, script: "zip -r ${LAMBDA_APP_ZIP_NAME} *", label: "Compressing files ..." )

                        sh (returnStdout: true, script: "aws s3 cp ${LAMBDA_APP_ZIP_NAME} s3://itl-0009-devops-all-s3-main-01/app-interchange/builds/${ENVIRONMENT}/lambda-app/", label: "Uploading to s3")

                        def scriptString = "aws lambda update-function-code \
                                          --function-name ${LAMBDA_APP_ARN} \
                                          --zip-file fileb://${LAMBDA_APP_ZIP_NAME}"
                        sh (returnStdout: false, script: scriptString, label: "Deploying Lambda App ..." )
                    }
                }
            }
        }

        stage('Deploying App') {
            steps {
                script {
                    def getOpenSearchSecret = sh (returnStdout: true, script: "aws secretsmanager get-secret-value --secret-id ${OPENSEARCH_SECRET} --output text --query SecretString", label: "Getting secret name ..." )
                    def jsonOSSecretResult = readJSON text: getOpenSearchSecret

                    def getRdsAuroraSecret = sh (returnStdout: true, script: "aws secretsmanager get-secret-value --secret-id ${RDS_AURORA_SECRET} --output text --query SecretString", label: "Getting secret name ..." )
                    def jsonRdsSecretResult = readJSON text: getRdsAuroraSecret

                    OPENSEARCH_USER = jsonOSSecretResult.username
                    OPENSEARCH_PASSWORD = jsonOSSecretResult.password
                    RDSAURORA_PASSWORD = jsonRdsSecretResult.password

                    wrap([$class: 'MaskPasswordsBuildWrapper',
                          varPasswordPairs: [[password: OPENSEARCH_PASSWORD],
                                             [password: RDSAURORA_PASSWORD]]]) {
                        sh 'sed -i -e "s;%DOMAIN_ENDPOINT%;' + OPENSEARCH_URL + ';g"\
                          -e "s;%USER%;' + OPENSEARCH_USER +';g" -e "s;%PASSWORD%;'+ scapeString(OPENSEARCH_PASSWORD) +';g" Config/logstash/filebeat-intelica.conf'

                        sh 'sed -i -e "s;%RDS_ENDPOINT%;' + RDS_INSTANCE_ENDPOINT + ';g"\
                            -e "s;%RDS_PASSWORD%;'+ scapeString(RDSAURORA_PASSWORD) +';g" .env'
                    }

                    sh (returnStdout: false, script: 'sed -i -e "s;%ENVIRONMENT%;' + ENVIRONMENT + ';g"\
                        ci/scripts/after_install.sh', label: "Replacing after install event...")

                    sh (returnStdout: false, script: "zip -r ${APP_ZIP_NAME} Module", label: "Compressing Module files ..." )

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} Build", label: "Compressing Build files ..." )

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} Config", label: "Compressing Config files ..." )

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} Dep", label: "Compressing Dep files ..." )

                    dir('ci'){
                        sh (returnStdout: false, script: "zip -ur ../${APP_ZIP_NAME} scripts appspec.yml", label: "Compressing CodeDeploy files ..." )
                    }

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} *.py .env", label: "Compressing python files ..." )

                    sh (returnStdout: false, script: "aws s3 cp ${APP_ZIP_NAME} s3://itl-0009-devops-all-s3-main-01/app-interchange/builds/${ENVIRONMENT}/ec2-app/", label: "Uploading to s3")

                    def deployment = sh (returnStdout: true, script: "aws deploy create-deployment \
                        --application-name Application-app-interchange \
                        --deployment-group-name Dg-app-interchange-${ENVIRONMENT} \
                        --file-exists-behavior OVERWRITE --ignore-application-stop-failures \
                        --s3-location bucket=itl-0009-devops-all-s3-main-01,bundleType=zip,key=app-interchange/builds/${ENVIRONMENT}/ec2-app/${APP_ZIP_NAME}", label: "Deploying App ...")

                    def jsonResult = readJSON text: deployment

                    awaitDeploymentCompletion("${jsonResult.deploymentId}")
                }
            }
        }
    }

    post {
       cleanup {
           cleanWs()
       }
     }
}

def getStackStatus(String stackName){
    def stackStatus

    try {
        def getStackStatus = sh (returnStdout: true, script: "aws cloudformation describe-stacks --stack-name ${stackName}", label: "Describe ${stackName} Stack").trim()
        def jsonResult = readJSON text: getStackStatus

        stackStatus = jsonResult.Stacks[0].StackStatus

    } catch (err) {
        echo "Stack No existe"
    }

    return stackStatus
}

def readYamlParameters(){

    def configValues = readYaml file: 'parameters.yaml'

    def stringParameterOverrides = "Environment=${ENVIRONMENT}"

    if(configValues[ENVIRONMENT] != null){

      configValues[ENVIRONMENT].each{
          key, value ->
          stringParameterOverrides = stringParameterOverrides + " ${key}=${value}"
      }
    }

    return stringParameterOverrides
}

def getEnvironment()
{
      def environment = 'test'
      def (vcs, org, project) = "${env.GIT_URL}".split('//')[-1].split('/')

      if(vcs == 'github.com' && org == 'MillennialV' && isMain()){
        environment = 'dev'
      }

      if(vcs == 'github.com'/* && org == 'Intelica-Interchange'*/){
          if (isDevelop()) {
            environment = 'dev'
          }

          if (isPre()) {
            environment = 'qas'
          }

          if (isMain()) {
            environment = 'prd'
          }
      }

      return environment
}

def isMain() {
  return env.BRANCH_NAME == 'main'
}

def isDevelop() {
  return env.BRANCH_NAME == 'dev'
}

def isPre() {
  return env.BRANCH_NAME == 'pre'
}

def getShortCommitId() {
    def gitCommit = env.GIT_COMMIT
    def shortGitCommit = "${gitCommit[0..6]}"
    return shortGitCommit
}

def scapeString(String s){
    return s.replaceAll(/\|/, "\\\\|")
    .replaceAll(/&/, "\\\\&")
    .replaceAll(/;/, "\\\\;")
    .replaceAll(/</, "\\\\<")
    .replaceAll(/>/, "\\\\>")
    .replaceAll(/\(/, "\\\\(")
    .replaceAll(/\)/, "\\\\)")
    .replaceAll(/\$/, "\\\\\$")
    .replaceAll(/`/, "\\\\`")
    .replaceAll(/"/, "\\\\\"")
    .replaceAll(/'/, "\\\\'")
}