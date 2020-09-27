/* ncftp_errno.h
 *
 * Copyright (c) 1996-2004 Mike Gleason, NcFTP Software.
 * All rights reserved.
 *
 */

#ifndef kNoErr
#	define kNoErr				0
#endif

#ifndef kErrGeneric
#	define kErrGeneric			(-1)
#endif

#define kErrFirst				(100)
#define kErrGetHostNameFailed			(-100)
#define kErrHostWithoutDomain			(-101)
#define kErrSetLinger				(-102)
#define kErrSetTypeOfService			(-103)
#define kErrSetOutOfBandInline			(-104)
#define kErrHostUnknown				(-105)
#define kErrNewStreamSocket			(-106)
#define kErrDupSocket				(-107)
#define kErrFdopenR				(-108)
#define kErrFdopenW				(-109)
#define kErrGetSockName				(-110)
#define kErrBindDataSocket			(-111)
#define kErrListenDataSocket			(-112)
#define kErrPassiveModeFailed			(-113)
#define kErrServerSentBogusPortNumber		(-114)
#define kErrConnectDataSocket			(-115)
#define kErrAcceptDataSocket			(-116)
#define kErrSetStartPoint			(-117)
#define kErrConnectMiscErr			(-118)
#define kErrConnectRetryableErr			(-119)
#define kErrConnectRefused			(-120)
#define kErrBadTransferType			(-121)
#define kErrInvalidDirParam			(-122)
#define kErrMallocFailed			(-123)
#define kErrPWDFailed				(-124)
#define kErrCWDFailed				(-125)
#define kErrRMDFailed				(-126)
#define kErrBadLineList				(-127)
#define kErrUnimplementedOption			(-128)
#define kErrUnimplementedFunction		(-129)
#define kErrLISTFailed				(-130)
#define kErrRETRFailed				(-131)
#define kErrSTORFailed				(-132)
#define kErrWriteFailed				(-133)
#define kErrReadFailed				(-134)
#define kErrSocketWriteFailed			(-135)
#define kErrSocketReadFailed			(-136)
#define kErrOpenFailed				(-137)
#define kErrBadMagic				(-138)
#define kErrBadParameter			(-139)
#define kErrMKDFailed				(-140)
#define kErrCannotGoToPrevDir			(-141)
#define kErrChmodFailed				(-142)
#define kErrUmaskFailed				(-143)
#define kErrDELEFailed				(-144)
#define kErrSIZEFailed				(-145)
#define kErrMDTMFailed				(-146)
#define kErrTYPEFailed				(-147)
#define kErrSIZENotAvailable			(-148)
#define kErrMDTMNotAvailable			(-149)
#define kErrRenameFailed			(-150)
#define kErrGlobFailed				(-151)
#define kErrSetKeepAlive			(-152)
#define kErrHostDisconnectedDuringLogin		(-153)
#define kErrBadRemoteUser			(-154)
#define kErrBadRemoteUserOrPassword		(-155)
#define kErrLoginFailed				(-156)
#define kErrInvalidReplyFromServer		(-157)
#define kErrRemoteHostClosedConnection		(-158)
#define kErrNotConnected			(-159)
#define kErrCouldNotStartDataTransfer		(-160)
#define kErrDataTransferFailed			(-161)
#define kErrPORTFailed				(-162)
#define kErrPASVFailed				(-163)
#define kErrUTIMEFailed				(-164)
#define kErrUTIMENotAvailable			(-165)
#define kErrHELPFailed				(-166)
#define kErrLocalDeleteFailed			(-167)
#define kErrLseekFailed				(-168)
#define kErrDataTransferAborted			(-169)
#define kErrSYMLINKFailed			(-170)
#define kErrSYMLINKNotAvailable			(-171)
#define kErrGlobNoMatch				(-172)
#define kErrFEATNotAvailable			(-173)
#define kErrNoValidFilesSpecified		(-174)
#define kErrNoBuf				(-175)
#define kErrLocalFileNewer			(-176)
#define kErrRemoteFileNewer			(-177)
#define	kErrLocalSameAsRemote			(-178)
#define kErrMLSDFailed				(-179)
#define kErrMLSTFailed				(-180)
#define kErrInvalidMLSTResponse			(-181)
#define kErrMLSTNotAvailable			(-182)
#define kErrMLSDNotAvailable			(-183)
#define kErrSTATFailed				(-184)
#define kErrSTATwithFileNotAvailable		(-185)
#define kErrNLSTFailed				(-186)
#define kErrNLSTwithFileNotAvailable		(-187)
#define kErrNoSuchFileOrDirectory		(-188)
#define kErrCantTellIfFileExists		(-189)
#define kErrFileExistsButCannotDetermineType	(-190)
#define kErrNotADirectory			(-191)
#define kErrRecursionLimitReached		(-192)
#define kErrControlTimedOut			(-193)
#define kErrDataTimedOut			(-194)
#define kErrUserCanceled			(-195)
#define kErrNoHostSpecified			(-196)
#define kErrRemoteSameAsLocal			(-197)
#define kErrProxyDataConnectionsDisabled	(-198)
#define kErrDataConnOriginatedFromBadPort	(-199)
#define kErrLast				(199)
