FROM ubuntu:xenial-20200114

# The popularity-contest is causing docker build to hang on apt install.
# cf. https://github.com/BRAINSia/BRAINSTools/issues/382
ARG DEBIAN_FRONTEND=noninteractive


# Install fmriprep dependances. - removed nodejs and bids validator, previously installed.
# cf. https://github.com/nipreps/fmriprep/blob/master/Dockerfile
# Prepare environment
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
                    curl \
                    bzip2 \
                    ca-certificates \
                    xvfb \
                    build-essential \
                    autoconf \
                    libtool \
                    pkg-config \
                    git && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Installing and setting up miniconda
RUN curl -sSLO https://repo.continuum.io/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh && \
    bash Miniconda3-4.5.11-Linux-x86_64.sh -b -p /usr/local/miniconda && \
    rm Miniconda3-4.5.11-Linux-x86_64.sh

# Set CPATH for packages relying on compiled libs (e.g. indexed_gzip)
ENV PATH="/usr/local/miniconda/bin:$PATH" \
    CPATH="/usr/local/miniconda/include/:$CPATH" \
    LANG="C.UTF-8" \
    LC_ALL="C.UTF-8" \
    PYTHONNOUSERSITE=1

# cf. https://docs.scipy.org/doc/scipy-1.1.0/reference/building/linux.html
# Install latest pandoc
RUN curl -o pandoc-2.2.2.1-1-amd64.deb -sSL "https://github.com/jgm/pandoc/releases/download/2.2.2.1/pandoc-2.2.2.1-1-amd64.deb" && \
    dpkg -i pandoc-2.2.2.1-1-amd64.deb && \
    rm pandoc-2.2.2.1-1-amd64.deb

# Installing freesurfer
RUN curl -sSL https://surfer.nmr.mgh.harvard.edu/pub/dist/freesurfer/6.0.1/freesurfer-Linux-centos6_x86_64-stable-pub-v6.0.1.tar.gz | tar zxv --no-same-owner -C /opt \
    --exclude='freesurfer/diffusion' \
    --exclude='freesurfer/docs' \
    --exclude='freesurfer/fsfast' \
    --exclude='freesurfer/lib/cuda' \
    --exclude='freesurfer/lib/qt' \
    --exclude='freesurfer/matlab' \
    --exclude='freesurfer/mni/share/man' \
    --exclude='freesurfer/subjects/fsaverage_sym' \
    --exclude='freesurfer/subjects/fsaverage3' \
    --exclude='freesurfer/subjects/fsaverage4' \
    --exclude='freesurfer/subjects/cvs_avg35' \
    --exclude='freesurfer/subjects/cvs_avg35_inMNI152' \
    --exclude='freesurfer/subjects/bert' \
    --exclude='freesurfer/subjects/lh.EC_average' \
    --exclude='freesurfer/subjects/rh.EC_average' \
    --exclude='freesurfer/subjects/sample-*.mgz' \
    --exclude='freesurfer/subjects/V1_average' \
    --exclude='freesurfer/trctrain'

ENV FSL_DIR="/usr/share/fsl/5.0" \
    OS="Linux" \
    FS_OVERRIDE=0 \
    FIX_VERTEX_AREA="" \
    FSF_OUTPUT_FORMAT="nii.gz" \
    FREESURFER_HOME="/opt/freesurfer"
ENV SUBJECTS_DIR="$FREESURFER_HOME/subjects" \
    FUNCTIONALS_DIR="$FREESURFER_HOME/sessions" \
    MNI_DIR="$FREESURFER_HOME/mni" \
    LOCAL_DIR="$FREESURFER_HOME/local" \
    MINC_BIN_DIR="$FREESURFER_HOME/mni/bin" \
    MINC_LIB_DIR="$FREESURFER_HOME/mni/lib" \
    MNI_DATAPATH="$FREESURFER_HOME/mni/data"
ENV PERL5LIB="$MINC_LIB_DIR/perl5/5.8.5" \
    MNI_PERL5LIB="$MINC_LIB_DIR/perl5/5.8.5" \
    PATH="$FREESURFER_HOME/bin:$FSFAST_HOME/bin:$FREESURFER_HOME/tktools:$MINC_BIN_DIR:$PATH"

# Installing Neurodebian packages (FSL, AFNI, git)
# RUN curl -sSL "http://neuro.debian.net/lists/$( lsb_release -c | cut -f2 ).us-ca.full" >> /etc/apt/sources.list.d/neurodebian.sources.list && \
#     apt-key add /usr/local/etc/neurodebian.gpg && \
#     (apt-key adv --refresh-keys --keyserver hkp://ha.pool.sks-keyservers.net 0xA5D32F012649A5A9 || true)

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget \
    && wget -O- http://neuro.debian.net/lists/xenial.de-fzj.full | tee /etc/apt/sources.list.d/neurodebian.sources.list \
    && apt-key adv --recv-keys --keyserver hkp://pool.sks-keyservers.net:80 0xA5D32F012649A5A9
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
                    fsl-core=5.0.9-5~nd16.04+1 \
                    fsl-mni152-templates=5.0.7-2 \
                    afni=16.2.07~dfsg.1-5~nd16.04+1 \
                    convert3d \
                    connectome-workbench=1.3.2-2~nd16.04+1 \
                    git-annex-standalone && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# install nodejs (for bids-validator)
# cf. https://github.com/nodesource/distributions/blob/master/README.md#installation-instructions
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash - \
    && apt-get install -y nodejs \
    && npm install npm@latest -g \
	&& rm -rf apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* 
# in/stall bids-validator.
RUN npm install -g bids-validator@1.5.6

ENV FSLDIR="/usr/share/fsl/5.0" \
    FSLOUTPUTTYPE="NIFTI_GZ" \
    FSLMULTIFILEQUIT="TRUE" \
    POSSUMDIR="/usr/share/fsl/5.0" \
    LD_LIBRARY_PATH="/usr/lib/fsl/5.0:$LD_LIBRARY_PATH" \
    FSLTCLSH="/usr/bin/tclsh" \
    FSLWISH="/usr/bin/wish" \
    AFNI_MODELPATH="/usr/lib/afni/models" \
    AFNI_IMSAVE_WARNINGS="NO" \
    AFNI_TTATLAS_DATASET="/usr/share/afni/atlases" \
    AFNI_PLUGINPATH="/usr/lib/afni/plugins"
ENV PATH="/usr/lib/fsl/5.0:/usr/lib/afni/bin:$PATH"

# Installing ANTs 2.3.4 (NeuroDocker build)
ENV ANTSPATH=/usr/lib/ants
RUN mkdir -p $ANTSPATH && \
    curl -sSL "https://dl.dropbox.com/s/gwf51ykkk5bifyj/ants-Linux-centos6_x86_64-v2.3.4.tar.gz" \
    | tar -xzC $ANTSPATH --strip-components 1
ENV PATH=$ANTSPATH:$PATH

# Installing SVGO
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y nodejs
RUN npm install -g svgo

# Installing and setting up ICA_AROMA
RUN mkdir -p /opt/ICA-AROMA && \
  curl -sSL "https://github.com/oesteban/ICA-AROMA/archive/v0.4.5.tar.gz" \
  | tar -xzC /opt/ICA-AROMA --strip-components 1 && \
  chmod +x /opt/ICA-AROMA/ICA_AROMA.py
ENV PATH="/opt/ICA-AROMA:$PATH" \
    AROMA_VERSION="0.4.5"

# Create a shared $HOME directory
RUN useradd -m -s /bin/bash -G users my_user
#USER my_user
WORKDIR /home/my_user
ENV HOME="/home/my_user"

# Installing precomputed python packages
# removed: 
RUN conda install -y python=3.7.1 \
                     pip=19.1 \
                     mkl=2018.0.3 \
                     mkl-service \
                     numpy=1.15.4 \
                     scipy=1.1.0 \
                     scikit-learn=0.19.1 \
                     matplotlib=2.2.2 \
                     pandas=0.23.4 \
                     libxml2=2.9.8 \
                     libxslt=1.1.32 \
                     graphviz=2.40.1 \
                     traits=4.6.0 \
                     zlib; sync && \
    chmod -R a+rX /usr/local/miniconda; sync && \
    chmod +x /usr/local/miniconda/bin/*; sync && \
    conda build purge-all; sync && \
    conda clean -tipsy && sync


# Unless otherwise specified each process should only use one thread - nipype
# will handle parallelization
ENV MKL_NUM_THREADS=1 \
    OMP_NUM_THREADS=1

# Precaching fonts, set 'Agg' as default backend for matplotlib
RUN python -c "from matplotlib import font_manager" && \
    sed -i 's/\(backend *: \).*$/\1Agg/g' $( python -c "import matplotlib; print(matplotlib.matplotlib_fname())" )

# Precaching atlases - install templateflow
RUN python -m pip install --no-cache-dir templateflow>0.6
RUN python -c "from templateflow import api as tfapi; \
               tfapi.get('MNI152NLin6Asym', resolution=(1, 2), suffix='T1w', desc=None); \
               tfapi.get('MNI152NLin6Asym', resolution=(1, 2), desc='brain', suffix='mask'); \
               tfapi.get('MNI152NLin2009cAsym', resolution=(1, 2), suffix='T1w', desc=None); \
               tfapi.get('MNI152NLin2009cAsym', resolution=(1, 2), desc='brain', suffix='mask'); \
               tfapi.get('MNI152NLin2009cAsym', resolution=1, desc='carpet', suffix='dseg'); \
               tfapi.get('MNI152NLin2009cAsym', resolution=1, label='brain', suffix='probseg'); \
               tfapi.get('MNI152NLin2009cAsym', resolution=2, desc='fMRIPrep', suffix='boldref'); \
               tfapi.get('OASIS30ANTs'); \
               tfapi.get('fsaverage', density='164k', desc='std', suffix='sphere'); \
               tfapi.get('fsaverage', density='164k', desc='vaavg', suffix='midthickness'); \
               tfapi.get('fsLR', density='32k'); \
               tfapi.get('MNI152NLin6Asym', resolution=2, atlas='HCP', suffix='dseg'); \
               tfapi.get('MNI152NLin6Asym'); \
               tfapi.get('MNI152NLin2009cAsym'); \
               tfapi.get('OASIS30ANTs'); \
               tfapi.get('fsaverage'); \
               tfapi.get('fsLR');"
RUN find $HOME/.cache/templateflow -type d -exec chmod go=u {} + && \
    find $HOME/.cache/templateflow -type f -exec chmod go=u {} +

# Install FMRIPREP
RUN python -m pip install fmriprep==20.2.0
ENV IS_DOCKER_8395080871=1

# Install SMRIPREP
# cf. https://pypi.org/project/smriprep/
RUN python -m pip install smriprep==0.7.0

# Install MRIQC
# cf. https://mriqc.readthedocs.io/en/latest/install.html + 
# https://pypi.org/project/mriqc/#history
RUN python -m pip install mriqc==0.15.2

RUN find $HOME -type d -exec chmod go=u {} + && \
    find $HOME -type f -exec chmod go=u {} + && \
    rm -rf $HOME/.npm $HOME/.conda $HOME/.empty
RUN ldconfig

# RUN conda update conda  # needed for conda activate

# Install our pipeline
COPY requirements.txt /preprocessing-src/requirements.txt
WORKDIR /preprocessing-src
RUN python -m pip install -r requirements.txt

COPY . /preprocessing-src

# Fix smriprep cf. https://github.com/nipreps/smriprep/issues/224
COPY smriprep-fasttrack-fix/ /usr/local/miniconda/lib/python3.7/site-packages/smriprep/smriprep/utils/  

# RUN apt-get install -y openmpi-bin

ENTRYPOINT ["python", "main.py"]

